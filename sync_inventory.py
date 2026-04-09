import os
import json
import time
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
RESULTS_FILE = "sync_results.csv"
STATE_FILE = "last_sent_state.json"
LOG_FILE = "sync_log.txt"
SUMMARY_FILE = "sync_summary.json"

DRY_RUN = False          # testing ke liye True kar sakte ho
VEEQO_PAGE_SIZE = 100    # products per page
NET32_BATCH_SIZE = 25    # Net32 max batch size
TARGET_WAREHOUSE_NAME = "GP-WH"


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


def load_mapping():
    if not os.path.exists(MAPPING_FILE):
        raise Exception(f"Mapping file not found: {MAPPING_FILE}")

    df = pd.read_csv(MAPPING_FILE, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    if "sku" not in df.columns or "mpid" not in df.columns:
        raise Exception("sku_mpid_map.csv must contain columns: sku, mpid")

    df["sku"] = df["sku"].astype(str).str.strip()
    df["mpid"] = df["mpid"].astype(str).str.strip()

    df = df[(df["sku"] != "") & (df["mpid"] != "")]
    return df


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def chunk_list(data, size):
    for i in range(0, len(data), size):
        yield data[i:i + size]


def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


# =========================
# VEEQO FETCH
# =========================

def get_veeqo_headers():
    return {
        "x-api-key": VEEQO_API_KEY,
        "Accept": "application/json"
    }


def get_gp_wh_on_hand_qty(sellable, target_warehouse_name=TARGET_WAREHOUSE_NAME):
    """
    Sirf target warehouse (GP-WH) ki on-hand qty nikaalta hai.
    JSON path:
    sellable -> stock_entries[] -> warehouse.name == 'GP-WH' -> physical_stock_level
    """
    stock_entries = sellable.get("stock_entries", [])

    if not isinstance(stock_entries, list):
        return 0

    for entry in stock_entries:
        warehouse = entry.get("warehouse", {}) or {}
        warehouse_name = str(warehouse.get("name", "")).strip()

        if warehouse_name == target_warehouse_name:
            return max(safe_int(entry.get("physical_stock_level", 0)), 0)

    return 0


def extract_veeqo_inventory_item(product):
    """
    Har product ke sellables me se:
    - SKU lo
    - GP-WH warehouse ki physical_stock_level lo
    """
    items = []
    sellables = product.get("sellables", [])

    if not isinstance(sellables, list):
        return items

    for sellable in sellables:
        sku = str(sellable.get("sku_code", "")).strip()
        if not sku:
            continue

        qty = get_gp_wh_on_hand_qty(sellable, TARGET_WAREHOUSE_NAME)

        items.append({
            "sku": sku,
            "qty": qty
        })

    return items


def fetch_all_veeqo_inventory():
    all_inventory = {}
    page = 1

    while True:
        url = f"{VEEQO_BASE_URL}/products?page={page}&page_size={VEEQO_PAGE_SIZE}"
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
            extracted_items = extract_veeqo_inventory_item(product)

            for item in extracted_items:
                sku = item["sku"]
                qty = item["qty"]
                all_inventory[sku] = qty

        if len(data) < VEEQO_PAGE_SIZE:
            break

        page += 1
        time.sleep(0.5)

    write_log(f"Total Veeqo SKUs fetched: {len(all_inventory)}")
    return all_inventory


# =========================
# NET32 UPDATE
# =========================

def get_net32_headers():
    return {
        "Subscription-Key": NET32_SUBSCRIPTION_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


def update_net32_batch(batch):
    payload = []

    for row in batch:
        payload.append({
            "mpid": int(row["mpid"]),
            "inventory": int(row["qty"])
        })

    if DRY_RUN:
        write_log(f"DRY RUN batch => {json.dumps(payload)}")
        return {
            "statusCode": 200,
            "payload": {
                "failureList": [],
                "totalSubmitted": len(payload),
                "totalSucceeded": len(payload)
            }
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
# MAIN SYNC
# =========================

def run_sync():
    write_log("========== SYNC STARTED ==========")
    write_log(f"Using warehouse for sync: {TARGET_WAREHOUSE_NAME}")
    write_log("Using quantity type: physical_stock_level (on-hand)")

    mapping_df = load_mapping()
    state = load_state()
    veeqo_inventory = fetch_all_veeqo_inventory()

    results_rows = []
    update_rows = []

    total_mapping = len(mapping_df)

    for _, row in mapping_df.iterrows():
        sku = str(row["sku"]).strip()
        mpid = str(row["mpid"]).strip()

        last_sent_qty = state.get(sku)
        last_run_time = now_utc_str()

        if sku not in veeqo_inventory:
            results_rows.append({
                "sku": sku,
                "mpid": mpid,
                "veeqo_qty": "",
                "last_sent_qty": last_sent_qty if last_sent_qty is not None else "",
                "update_needed": "no",
                "status": "not_found_in_veeqo",
                "message": f"SKU not found in Veeqo for warehouse {TARGET_WAREHOUSE_NAME}",
                "last_run_time": last_run_time
            })
            continue

        current_qty = safe_int(veeqo_inventory[sku], 0)

        if last_sent_qty is not None:
            last_sent_qty = safe_int(last_sent_qty, None)

        if last_sent_qty is not None and current_qty == last_sent_qty:
            results_rows.append({
                "sku": sku,
                "mpid": mpid,
                "veeqo_qty": current_qty,
                "last_sent_qty": last_sent_qty,
                "update_needed": "no",
                "status": "unchanged",
                "message": "Quantity unchanged, skipped",
                "last_run_time": last_run_time
            })
        else:
            results_rows.append({
                "sku": sku,
                "mpid": mpid,
                "veeqo_qty": current_qty,
                "last_sent_qty": last_sent_qty if last_sent_qty is not None else "",
                "update_needed": "yes",
                "status": "pending",
                "message": "Ready for Net32 update",
                "last_run_time": last_run_time
            })

            update_rows.append({
                "sku": sku,
                "mpid": mpid,
                "qty": current_qty
            })

    write_log(f"Total mapping rows: {total_mapping}")
    write_log(f"Rows needing update: {len(update_rows)}")

    success_mpids = set()
    failed_mpids = set()
    failed_messages = {}

    for batch in chunk_list(update_rows, NET32_BATCH_SIZE):
        try:
            result = update_net32_batch(batch)
            write_log(f"Net32 response: {result}")

            payload = result.get("payload", {}) if isinstance(result, dict) else {}
            failure_list = payload.get("failureList", []) or []

            batch_mpids = {str(x["mpid"]) for x in batch}
            batch_failed = {str(x) for x in failure_list}
            batch_success = batch_mpids - batch_failed

            for mpid in batch_success:
                success_mpids.add(mpid)

            for mpid in batch_failed:
                failed_mpids.add(mpid)
                failed_messages[mpid] = "Returned in failureList"

        except Exception as e:
            write_log(f"Batch failed completely: {str(e)}")
            for x in batch:
                failed_mpids.add(str(x["mpid"]))
                failed_messages[str(x["mpid"])] = str(e)

        time.sleep(0.5)

    new_state = dict(state)

    for row in results_rows:
        mpid = str(row["mpid"])
        sku = str(row["sku"])

        if row["status"] == "pending":
            if mpid in success_mpids:
                row["status"] = "success"
                row["message"] = "Updated successfully in Net32"
                row["last_sent_qty"] = row["veeqo_qty"]
                new_state[sku] = row["veeqo_qty"]

            elif mpid in failed_mpids:
                row["status"] = "failed"
                row["message"] = failed_messages.get(mpid, "Net32 update failed")

            else:
                row["status"] = "unknown"
                row["message"] = "No final status returned"

    save_state(new_state)

    results_df = pd.DataFrame(results_rows)
    results_df.to_csv(RESULTS_FILE, index=False)

    summary = {
        "warehouse_used": TARGET_WAREHOUSE_NAME,
        "quantity_type": "physical_stock_level",
        "total_mapping_rows": int(total_mapping),
        "found_in_veeqo": int((results_df["status"] != "not_found_in_veeqo").sum()),
        "not_found_in_veeqo": int((results_df["status"] == "not_found_in_veeqo").sum()),
        "unchanged": int((results_df["status"] == "unchanged").sum()),
        "success": int((results_df["status"] == "success").sum()),
        "failed": int((results_df["status"] == "failed").sum()),
        "unknown": int((results_df["status"] == "unknown").sum())
    }

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    write_log(f"Summary: {summary}")
    write_log("========== SYNC FINISHED ==========")


if __name__ == "__main__":
    run_sync()