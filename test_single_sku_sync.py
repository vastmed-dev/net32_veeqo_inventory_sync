import os
import json
from datetime import datetime, timezone

import requests
import pandas as pd

# =========================
# CONFIG
# =========================

VEEQO_API_KEY = "Vqt/06cac2a6c529100d2aaa79b029e97f58"
NET32_SUBSCRIPTION_KEY = "ea40c7bd2b454b7a948594180c1cbd58"

VEEQO_BASE_URL = "https://api.veeqo.com"
NET32_INVENTORY_URL = "https://api.net32.com/inventory/products/update"

MAPPING_FILE = "sku_mpid_map.csv"
LOG_FILE = "test_single_sku_log.txt"

TEST_SKU = "VMMSKIL3P50"
TARGET_WAREHOUSE_NAME = "GP-WH"

DRY_RUN = False   # pehle True rakho, jab check ho jaye to False kar dena


# =========================
# HELPERS
# =========================

def now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def write_log(message):
    line = f"[{now_utc_str()}] {message}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def get_veeqo_headers():
    return {
        "x-api-key": VEEQO_API_KEY,
        "Accept": "application/json"
    }


def get_net32_headers():
    return {
        "Subscription-Key": NET32_SUBSCRIPTION_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


# =========================
# LOAD MPID FROM CSV
# =========================

def get_mpid_for_sku(sku):
    if not os.path.exists(MAPPING_FILE):
        raise Exception(f"Mapping file not found: {MAPPING_FILE}")

    df = pd.read_csv(MAPPING_FILE, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    if "sku" not in df.columns or "mpid" not in df.columns:
        raise Exception("sku_mpid_map.csv must contain columns: sku, mpid")

    df["sku"] = df["sku"].astype(str).str.strip()
    df["mpid"] = df["mpid"].astype(str).str.strip()

    match = df[df["sku"] == sku]

    if match.empty:
        raise Exception(f"SKU not found in mapping file: {sku}")

    mpid = str(match.iloc[0]["mpid"]).strip()

    if not mpid:
        raise Exception(f"MPID is empty for SKU: {sku}")

    return mpid


# =========================
# VEEQO FETCH
# =========================

def find_sku_in_veeqo(target_sku):
    """
    Veeqo products pages me search karega aur target SKU milne par
    uska GP-WH warehouse ka physical_stock_level return karega.
    """
    page = 1
    page_size = 100

    while True:
        url = f"{VEEQO_BASE_URL}/products?page={page}&page_size={page_size}"
        write_log(f"Fetching Veeqo page {page}")

        response = requests.get(url, headers=get_veeqo_headers(), timeout=60)

        if response.status_code != 200:
            raise Exception(
                f"Veeqo API failed on page {page}: {response.status_code} - {response.text}"
            )

        data = response.json()

        if not isinstance(data, list):
            raise Exception("Unexpected Veeqo response. Expected a list of products.")

        if len(data) == 0:
            break

        for product in data:
            sellables = product.get("sellables", [])
            if not isinstance(sellables, list):
                continue

            for sellable in sellables:
                sku = str(sellable.get("sku_code", "")).strip()
                if sku != target_sku:
                    continue

                stock_entries = sellable.get("stock_entries", [])
                gp_wh_qty = 0

                if isinstance(stock_entries, list):
                    for entry in stock_entries:
                        warehouse = entry.get("warehouse", {}) or {}
                        warehouse_name = str(warehouse.get("name", "")).strip()

                        if warehouse_name == TARGET_WAREHOUSE_NAME:
                            gp_wh_qty = max(safe_int(entry.get("physical_stock_level", 0)), 0)
                            break

                return {
                    "found": True,
                    "sku": sku,
                    "qty": gp_wh_qty,
                    "product_title": str(product.get("title", "")).strip(),
                    "sellable_id": sellable.get("id"),
                }

        if len(data) < page_size:
            break

        page += 1

    return {
        "found": False,
        "sku": target_sku,
        "qty": None
    }


# =========================
# NET32 UPDATE
# =========================

def update_net32_single(mpid, qty):
    payload = [
        {
            "mpid": int(mpid),
            "inventory": int(qty)
        }
    ]

    if DRY_RUN:
        write_log(f"DRY RUN payload => {json.dumps(payload)}")
        return {
            "status": "dry_run",
            "payload": payload
        }

    response = requests.post(
        NET32_INVENTORY_URL,
        headers=get_net32_headers(),
        json=payload,
        timeout=60
    )

    try:
        result = response.json()
    except Exception:
        result = {"raw_text": response.text}

    if response.status_code != 200:
        raise Exception(f"Net32 API failed: {response.status_code} - {result}")

    return result


# =========================
# MAIN TEST
# =========================

def run_test():
    write_log("========== SINGLE SKU TEST STARTED ==========")
    write_log(f"Test SKU: {TEST_SKU}")
    write_log(f"Warehouse used: {TARGET_WAREHOUSE_NAME}")
    write_log("Quantity type used: physical_stock_level (on-hand)")

    mpid = get_mpid_for_sku(TEST_SKU)
    write_log(f"Mapping file MPID found: {mpid}")

    veeqo_result = find_sku_in_veeqo(TEST_SKU)

    if not veeqo_result["found"]:
        raise Exception(f"SKU not found in Veeqo: {TEST_SKU}")

    qty = veeqo_result["qty"]
    write_log(f"Veeqo SKU found: {TEST_SKU}")
    write_log(f"Product title: {veeqo_result['product_title']}")
    write_log(f"GP-WH on-hand qty: {qty}")

    net32_result = update_net32_single(mpid, qty)
    write_log(f"Net32 result: {net32_result}")

    write_log("========== SINGLE SKU TEST FINISHED ==========")


if __name__ == "__main__":
    run_test()