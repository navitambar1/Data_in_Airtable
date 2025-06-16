import requests
from datetime import datetime
import pytz
import urllib.parse

SOURCE_BASE_ID = "appanUA6rMslh7laz"     # Base ID for Properties & Phone_Number_Info
TARGET_BASE_ID = "appIOiFaquIX9Vqxp"     # Base ID where Owners table exists
API_KEY = "patvxifFmc7WmAvRY.6825c809405ff4f84d308f0f07412fe7be6f1082e2ad4d425e7c62f5394faf9d"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def now_ny():
    tz = pytz.timezone("America/New_York")
    return datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

def fetch_all_records(base_id, table_name):
    url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
    all_records = []
    offset = None

    while True:
        params = {"offset": offset} if offset else {}
        response = requests.get(url, headers=HEADERS, params=params)
        data = response.json()
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    # print(f"Fetched {len(all_records)} properties records and records are {all_records}")
    return all_records


def build_filter_by_formula(field_name, value):
    # Escape single quotes by doubling them per Airtable formula rules
    escaped_value = value.replace("'", "\\'")
    formula = f"{{{field_name}}} = '{escaped_value}'"
    return urllib.parse.quote(formula)


def create_owner_records(properties, phones):
    owners_table_url = f"https://api.airtable.com/v0/{TARGET_BASE_ID}/tblQwEpbUXafJ46I0"
    case_to_phone = {
        p["fields"].get("case_number"): p["fields"]
        for p in phones if "case_number" in p["fields"]
    }

    for record in properties:
        fields = record.get("fields", {})
        case_number = fields.get("case_number")
        owner_name = fields.get("owner_name", "").strip()

        if not case_number or not owner_name:
            continue

        # Step 1: Check if owner already exists
        encoded_formula = build_filter_by_formula("Full Name", owner_name)
        check_url = f"{owners_table_url}?filterByFormula={encoded_formula}"
        check_response = requests.get(check_url, headers=HEADERS)

        if check_response.status_code != 200:
            print(f"⚠️ Error checking for existing owner '{owner_name}': {check_response.text}")
            continue

        existing = check_response.json().get("records", [])
        if existing:
            print(f"⚠️ Skipping owner '{owner_name}' — already exists.")
            continue

        # Step 2: Prepare payload
        phone_fields = case_to_phone.get(case_number, {})
        record_id = record.get("case_number")
        owner_payload = {
            "fields": {
                "case_number": case_number,
                "Full Name": owner_name,
                "First name": fields.get("first_name", ""),
                "Last name": fields.get("last_name", ""),
                "Mailing Address": fields.get("owner_mailing_address", ""),
                "Phone 1": phone_fields.get("phone_no1", ""),
                "Phone 2": phone_fields.get("phone_no2", ""),
                "Phone 3": phone_fields.get("phone_no3", ""),
                "Deceased ?": fields.get("owner_deceased", False),
                "🏠 Properties Table": case_number,
                "Lead Stage": "",
                "Research Assigned To": [],
                "Owner Notes (Manager)": "",
                "Transcript Summary": [],
                "Next Action": "Follow Up"
            }
        }

        # Step 3: Insert into Airtable
        print(f"Creating owner for case {case_number}: {owner_payload}")
        response = requests.post(owners_table_url, headers=HEADERS, json={"records": [owner_payload]})
        if response.status_code != 200:
            print(f"❌ Failed to create owner '{owner_name}': {response.text}")
        else:
            print(f"✅ Owner '{owner_name}' created for case {case_number}")


# -----------------------------
# Run the integration process
# -----------------------------
if __name__ == "__main__":
    properties = fetch_all_records(SOURCE_BASE_ID, "property_info")
    phones = fetch_all_records(SOURCE_BASE_ID, "phone_number_info")
    create_owner_records(properties, phones)
