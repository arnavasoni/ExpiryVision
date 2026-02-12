import json
import pandas as pd
import win32com.client as win32
from datetime import datetime

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

ACTIONS_JSON_PATH = r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DWT_ExpiryVision - Documents\Outputs\expiry_actions.json"
BATCH_XLSX_PATH = r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DWT_ExpiryVision - Documents\Data\batch_details.xlsx"

# ---------------------------------------------------------
# EMAIL BUILDER
# ---------------------------------------------------------

def build_email_body(vendor_name, batches):
    lines = [
        f"Dear {vendor_name},",
        "",
        "The following batches are nearing expiry.",
        "Please confirm whether revalidation is possible.",
        "",
        "PLEASE REPLY BELOW THIS LINE ONLY",
        ""
    ]

    for b in batches:
        lines.extend([
            f"Batch No.: {b['batch_number']}",
            "To revalidate?: YES / NO",
            "Revised Expiry Date: YYYY-MM-DD",
            ""
        ])

    lines.extend([
        "Regards,",
        "Expiry Vision System"
    ])

    return "\n".join(lines)


# ---------------------------------------------------------
# SEND EMAILS
# ---------------------------------------------------------

def send_vendor_emails():
    with open(ACTIONS_JSON_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    outlook = win32.Dispatch("Outlook.Application")
    df = pd.read_excel(BATCH_XLSX_PATH, engine="openpyxl")

    today = datetime.today().strftime("%Y-%m-%d")

    for vendor_data in payload["vendors"].values():
        vendor = vendor_data["vendor_canonical_name"]
        email = vendor_data["vendor_email"]
        batches = vendor_data["batches"]

        mail = outlook.CreateItem(0)
        mail.To = email
        mail.Subject = f"Expiry Revalidation Request | {vendor}"
        mail.Body = build_email_body(vendor, batches)

        # mail.Send()
        mail.Save()

        # Update last_notified_date in Excel
        for batch in batches:
            df.loc[
                df["batch_number"] == batch["batch_number"],
                "last_notified_date"
            ] = today

        print(f"Email sent to {vendor} ({email})")

    df.to_excel(BATCH_XLSX_PATH, index=False, engine="openpyxl")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    send_vendor_emails()
