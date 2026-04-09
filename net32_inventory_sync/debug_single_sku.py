import json
import requests

VEEQO_API_KEY = "Vqt/06cac2a6c529100d2aaa79b029e97f58"
VEEQO_BASE_URL = "https://api.veeqo.com"
TARGET_SKU = "VMWDWT151514"

def get_veeqo_headers():
    return {
        "x-api-key": VEEQO_API_KEY,
        "Accept": "application/json"
    }

page = 1
page_size = 100
found = False

while True:
    url = f"{VEEQO_BASE_URL}/products?page={page}&page_size={page_size}"
    print(f"Checking page {page} ...")
    response = requests.get(url, headers=get_veeqo_headers(), timeout=60)
    response.raise_for_status()
    data = response.json()

    if not data:
        break

    for product in data:
        sellables = product.get("sellables", [])
        if isinstance(sellables, list):
            for sellable in sellables:
                sku = str(sellable.get("sku_code", "")).strip()
                if sku == TARGET_SKU:
                    found = True
                    with open("single_sku_debug.json", "w", encoding="utf-8") as f:
                        json.dump(sellable, f, indent=2, ensure_ascii=False)
                    print("Found SKU. Saved to single_sku_debug.json")
                    print(json.dumps(sellable, indent=2, ensure_ascii=False))
                    break
        if found:
            break

    if found:
        break

    if len(data) < page_size:
        break

    page += 1

if not found:
    print("SKU not found")