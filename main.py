import requests
from datetime import datetime
import pytz

# Constants
TARGET_BASE_ID = "appIOiFaquIX9Vqxp"
PAT = "patvxifFmc7WmAvRY.b318f6a6f60a3f05c4db627d949c8145fa87bcbecf2fad760e2696b4f1c222ec"
HEADERS = {
    "Authorization": f"Bearer {PAT}",
    "Content-Type": "application/json"
}
BATCH_SIZE = 10
AIRTABLE_API_URL = "https://api.airtable.com/v0"

# Simulated Airtable local table records (Replace with actual Airtable API fetch)
case_intake_records = []  
property_info_records = []
tax_info_records = []

# Helper
def now_ny():
    return datetime.now(pytz.timezone("America/New_York")).strftime("%m/%d/%Y, %I:%M:%S %p")

def fetch_existing_properties(case_number):
    url = f"{AIRTABLE_API_URL}/{TARGET_BASE_ID}/Properties"
    params = {
        "filterByFormula": f"ENCODE_URL_COMPONENT({{property_id / case_number}})='{case_number}'"
    }
    response = requests.get(url, headers=HEADERS, params=params)
    return response.json() if response.ok else None

def build_property_data(case, case_number, prop_info, tax_info):
    fields = {
        "Property ID / Case No.": case_number,
        "County": case.get("County", ""),
        "Filing Date": case.get("Filing_date", None),
        "Property Status": "New",
        "Create Date": now_ny(),
        "Last Updated Date": now_ny()
    }
    if prop_info:
        fields.update({
            "Property Address": prop_info.get("property_address", ""),
            "Parcel ID": prop_info.get("Parcel_or_tax_id", ""),
            "🧑‍⚖️ Owner Deceased Reason": prop_info.get("Owner Deceased Reason", ""),
            "👨‍👩‍👧 Number of Heirs": prop_info.get("Number of Heirs", None)
        })
    if tax_info:
        fields.update({
            "Assessed Value": tax_info.get("Assessed_value", None),
            "Equity Status": tax_info.get("Equity Status", "")
        })
    return {"fields": fields}

def build_tax_data(case_number, tax_info, prop_id):
    return {
        "fields": {
            "case_number": case_number,
            "Tax Year": 2024 if case_number.startswith("24") else (2025 if case_number.startswith("25") else None),
            "Property Tax Due": tax_info.get("total_tax_value", None),
            "Mortgage Balance": tax_info.get("Mortgage_Balance", None),
            "🏠 Properties Table": [{"id": prop_id}],
            "Create Date": now_ny(),
            "Last Updated Date": now_ny()
        }
    }

def build_owner_data(case_number, owners, prop_id):
    return [
        {
            "fields": {
                "case_number": case_number,
                "Full Name": o.get("Owner Name", ""),
                "First Name": o.get("First_Name", ""),
                "Last Name": o.get("Last_Name", ""),
                "Mailing Address": o.get("Owner_Mailing_Address", ""),
                "Phone 1": o.get("Phone_no1", ""),
                "Phone 2": o.get("Phone_no2", ""),
                "Phone 3": o.get("Phone_no3", ""),
                "Deceased ?": o.get("Owner Deceased", False),
                "🏠 Properties Table": [{"id": prop_id}],
                "Create Date": now_ny(),
                "Last Updated Date": now_ny()
            }
        } for o in owners
    ]

def create_record(table, data):
    url = f"{AIRTABLE_API_URL}/{TARGET_BASE_ID}/{table}"
    response = requests.post(url, headers=HEADERS, json={"records": data if isinstance(data, list) else [data]})
    return response.json() if response.ok else None

def process_case(case):
    case_number = case.get("case_number")
    if not case_number:
        print("Skipping record without case number.")
        return

    existing = fetch_existing_properties(case_number)
    if existing and existing.get("records"):
        print(f"Property already exists for case {case_number}")
        return

    prop_info = next((r for r in property_info_records if r.get("case_number") == case_number), None)
    tax_info = next((r for r in tax_info_records if r.get("case_number") == case_number), None)
    owners = [r for r in property_info_records if r.get("case_number") == case_number]

    prop_data = build_property_data(case, case_number, prop_info, tax_info)
    prop_resp = create_record("Properties", prop_data)
    if not prop_resp:
        print(f"Failed to create property for {case_number}")
        return

    prop_id = prop_resp["records"][0]["id"]

    if tax_info:
        tax_data = build_tax_data(case_number, tax_info, prop_id)
        create_record("Tax Info", tax_data)

    if owners:
        owner_data = build_owner_data(case_number, owners, prop_id)
        create_record("Owners", owner_data)

    print(f"✅ Case {case_number} processed successfully.")

# Entry point
for case in case_intake_records[:BATCH_SIZE]:
    process_case(case)
