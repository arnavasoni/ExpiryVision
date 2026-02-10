import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from collections import Counter

# ---------------------------------------------------------
# CONFIG (DWT – Excel)
# ---------------------------------------------------------

BATCH_XLSX_PATH = r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DWT_ExpiryVision - Documents\Data\batch_details.xlsx"
OUTPUT_JSON_PATH = r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DWT_ExpiryVision - Documents\Outputs\expiry_actions.json"

ALERT_DAYS = 20
CRITICAL_DAYS = 10

# ---------------------------------------------------------
# DATE UTILITIES
# ---------------------------------------------------------

def parse_date(date_str):
    """Parse string to date, handle multiple formats."""
    if not date_str or pd.isna(date_str):
        return None

    for fmt in ("%Y-%m-%d", "%d-%m-%y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            continue

    return None


def calculate_status(expiry_date_str, revised_expiry_date_str):
    """Calculate batch status and days pending."""
    expiry_date = parse_date(expiry_date_str)
    revised_expiry_date = parse_date(revised_expiry_date_str)
    effective_expiry = revised_expiry_date or expiry_date

    if not effective_expiry:
        return None, None, None

    today = datetime.today().date()
    days_left = (effective_expiry - today).days

    if days_left < CRITICAL_DAYS:
        status = "CRITICAL"
    elif days_left <= ALERT_DAYS:
        status = "ALERT"
    else:
        status = "NORMAL"

    return status, days_left, effective_expiry.strftime("%Y-%m-%d")

# ---------------------------------------------------------
# CORE DECISION ENGINE
# ---------------------------------------------------------

def build_expiry_decisions(xlsx_path=BATCH_XLSX_PATH):
    df = pd.read_excel(xlsx_path, engine="openpyxl")

    required_cols = [
        "batch_number",
        "expiry_date",
        "revised_expiry_date",
        "vendor_canonical_name",
        "vendor_email",
        "total_quantity",
        "last_notified_date",
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    decisions = []

    for _, row in df.iterrows():
        status, days_left, effective_expiry = calculate_status(
            row.get("expiry_date"),
            row.get("revised_expiry_date")
        )

        if status not in ("CRITICAL", "ALERT"):
            continue

        vendor = str(row.get("vendor_canonical_name") or "").strip()
        email = str(row.get("vendor_email") or "").strip()
        batch_number = str(row.get("batch_number") or "").strip()

        if not vendor or not email:
            print(f"Skipping batch '{batch_number}' due to missing vendor mapping or email.")
            continue

        decisions.append({
            "vendor_canonical_name": vendor,
            "vendor_email": email,
            "batch_number": batch_number,
            "expiry_date": str(row.get("expiry_date") or ""),
            "revised_expiry_date": str(row.get("revised_expiry_date") or ""),
            "effective_expiry_date": effective_expiry,
            "days_pending": days_left,
            "status": status,
            "total_quantity": str(row.get("total_quantity") or "")
        })

    return decisions

# ---------------------------------------------------------
# GROUP BY VENDOR (POWER AUTOMATE FRIENDLY)
# ---------------------------------------------------------

def group_by_vendor(decisions):
    vendor_payload = {}

    for item in decisions:
        vendor = item["vendor_canonical_name"]

        if vendor not in vendor_payload:
            vendor_payload[vendor] = {
                "vendor_canonical_name": vendor,
                "vendor_email": item["vendor_email"],
                "batches": []
            }

        vendor_payload[vendor]["batches"].append({
            "batch_number": item["batch_number"],
            "expiry_date": item["expiry_date"],
            "revised_expiry_date": item["revised_expiry_date"],
            "effective_expiry_date": item["effective_expiry_date"],
            "days_pending": item["days_pending"],
            "status": item["status"],
            "total_quantity": item["total_quantity"]
        })

    for data in vendor_payload.values():
        data["batches"].sort(key=lambda x: x["effective_expiry_date"])

    return vendor_payload

# ---------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------

def write_json(payload, output_path=OUTPUT_JSON_PATH):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_on": datetime.today().strftime("%Y-%m-%d"),
                "alert_days": ALERT_DAYS,
                "critical_days": CRITICAL_DAYS,
                "vendors": payload,
            },
            f,
            indent=2
        )

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    decisions = build_expiry_decisions()
    vendor_payload = group_by_vendor(decisions)
    write_json(vendor_payload)

    print(f"Expiry decision JSON generated: {OUTPUT_JSON_PATH}")
    print(f"Vendors requiring action: {len(vendor_payload)}")

    status_counts = Counter(
        [b["status"] for v in vendor_payload.values() for b in v["batches"]]
    )
    print("Status counts:", dict(status_counts))
