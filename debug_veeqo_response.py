import json
import requests

VEEQO_API_KEY = "Vqt/06cac2a6c529100d2aaa79b029e97f58"
VEEQO_BASE_URL = "https://api.veeqo.com"

def get_veeqo_headers():
    return {
        "x-api-key": VEEQO_API_KEY,
        "Accept": "application/json"
    }

url = f"{VEEQO_BASE_URL}/products?page=1&page_size=5"
response = requests.get(url, headers=get_veeqo_headers(), timeout=60)

print("Status Code:", response.status_code)

try:
    data = response.json()
    print("Type:", type(data))
    with open("veeqo_page1_debug.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("Saved to veeqo_page1_debug.json")
except Exception as e:
    print("JSON parse error:", e)
    print(response.text)