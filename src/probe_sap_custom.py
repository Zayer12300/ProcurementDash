import json, requests
from requests.auth import HTTPBasicAuth
from pathlib import Path

# Load ASMO Credentials from your config folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CRED_PATH = PROJECT_ROOT / "config" / "Credentials.json"

def get_sap_config():
    with open(CRED_PATH, 'r') as f:
        data = json.load(f)
    return data['sap_connections'][0]

def search_sap_comprehensive(search_val: str):
    cfg = get_sap_config()
    base_url = cfg['base_url'].rstrip('/')
    service = cfg['service'].strip('/')
    
    # Logic: 45 series = Header (Ext Ref), 50 series = Item (Purchase Requisition)
    if search_val.startswith("45"):
        entity = "I_PurchaseOrder"
        field = "CorrespncExternalReference"
        select = "PurchaseOrder,CorrespncExternalReference,Supplier"
    else:
        entity = "I_PurchaseOrderItem"
        field = "PurchaseRequisition"
        select = "PurchaseOrder,PurchaseOrderItem,PurchaseRequisition,Material"

    url = f"{base_url}/{service}/{entity}"
    params = {
        "$format": "json",
        "$top": 10,
        "$select": select,
        "$filter": f"startswith({field}, '{search_val}')"
    }

    print(f"\n📡 SEARCHING SAP {entity} FOR: {search_val}")
    try:
        r = requests.get(url, params=params, auth=HTTPBasicAuth(cfg['username'], cfg['password']), verify=False)
        if r.status_code == 200:
            results = r.json().get('d', {}).get('results', [])
            if not results:
                print(f"❌ No matches found for '{search_val}' in field '{field}'")
            for item in results:
                if "CorrespncExternalReference" in item:
                    print(f"✅ [PO HEADER] PO: {item['PurchaseOrder']} | Customer PO (45s): {item['CorrespncExternalReference']}")
                else:
                    print(f"✅ [PO ITEM]   PO: {item['PurchaseOrder']} | Item: {item['PurchaseOrderItem']} | Customer PR (50s): {item['PurchaseRequisition']}")
        else:
            print(f"⚠️ SAP Error {r.status_code}: {r.text}")
    except Exception as e:
        print(f"🔥 Connection Failed: {e}")

# --- EXECUTION ---
search_sap_comprehensive("4507") # Search PO Series
search_sap_comprehensive("5021") # Search PR Series