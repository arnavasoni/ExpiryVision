import sys
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import time
from bs4 import BeautifulSoup

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

BATCH_XLSX_PATH = r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DWT_ExpiryVision - Documents\Data\batch_details.xlsx"

# ---------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------

def clean_email_body(raw_body: str) -> str:
    """
    Remove HTML, scripts, styles, signatures.
    Returns clean plain text.
    """
    if not raw_body:
        return ""

    soup = BeautifulSoup(raw_body, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def extract_vendor_reply(cleaned_text: str) -> str:
    """
    Extract only the vendor's reply part from the email.
    Cuts off everything after template / quoted section.
    """
    split_patterns = [
        "PLEASE REPLY BELOW THIS LINE ONLY",
        "On .* wrote:",
        "Regards,",
    ]
    for pattern in split_patterns:
        match = re.search(pattern, cleaned_text, re.IGNORECASE)
        if match:
            cleaned_text = cleaned_text[:match.start()]
    return cleaned_text.strip()


def parse_vendor_response(email_text: str):
    """
    Extract batch responses from cleaned email text.
    Returns dict:
    {
        batch_no: {
            "revalidate": "YES"/"NO",
            "revised_expiry_date": "YYYY-MM-DD" or None
        }
    }
    """
    # Match Batch blocks with valid YYYY-MM-DD dates
    pattern = re.compile(
        r"Batch\s*No\.?\s*:\s*(?P<batch>[^\s]+(?:\s*/\s*\S+)?)\s*"
        r"To\s*revalidate\?\s*:\s*(?P<reval>YES|NO)\s*"
        r"(?:Revised\s*Expiry\s*Date\s*:\s*(?P<date>\d{4}-\d{2}-\d{2}))?",
        re.IGNORECASE
    )

    responses = {}

    for match in pattern.finditer(email_text):
        batch = match.group("batch").strip()
        reval = match.group("reval").upper()
        date = match.group("date")

        # Skip template placeholders
        if date == "YYYY-MM-DD":
            date = None

        # Only take the first valid response per batch
        if batch not in responses:
            responses[batch] = {
                "revalidate": reval,
                "revised_expiry_date": date
            }

    return responses


def parse_date(date_str):
    if not date_str or pd.isna(date_str):
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d-%m-%y"):
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            continue
    return None

# ---------------------------------------------------------
# CORE UPDATE ENGINE
# ---------------------------------------------------------

def apply_vendor_reply(raw_email_body: str):
    df = pd.read_excel(BATCH_XLSX_PATH, engine="openpyxl")

    # Extract only vendor reply portion
    email_text = clean_email_body(raw_email_body)
    vendor_reply_text = extract_vendor_reply(email_text)

    batch_responses = parse_vendor_response(vendor_reply_text)
    batch_responses_clean = {k.strip(): v for k, v in batch_responses.items()}

    today_str = datetime.today().strftime("%Y-%m-%d")
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated_batches = 0

    for idx, row in df.iterrows():
        batch_no_excel = str(row.get("batch_number")).strip()

        if batch_no_excel not in batch_responses_clean:
            continue

        response = batch_responses_clean[batch_no_excel]
        reval_flag = response["revalidate"]
        revised_date = response["revised_expiry_date"]

        df.at[idx, "revalidation_status"] = reval_flag
        df.at[idx, "last_vendor_response_date"] = today_str
        df.at[idx, "revalidation_timestamp"] = now_ts

        original_expiry = parse_date(row.get("expiry_date"))

        if reval_flag == "YES" and revised_date:
            revised_expiry = parse_date(revised_date)
            if revised_expiry:
                df.at[idx, "revised_expiry_date"] = revised_expiry.strftime("%Y-%m-%d")
                df.at[idx, "effective_expiry_date"] = revised_expiry.strftime("%Y-%m-%d")
            else:
                df.at[idx, "effective_expiry_date"] = original_expiry.strftime("%Y-%m-%d")
        else:
            df.at[idx, "effective_expiry_date"] = original_expiry.strftime("%Y-%m-%d")

        updated_batches += 1

    df.to_excel(BATCH_XLSX_PATH, index=False, engine="openpyxl")
    print(f"Vendor reply processed. Batches updated: {updated_batches}")


if __name__ == "__main__":
    try:
        email_file_path = sys.argv[1] if len(sys.argv) >= 2 else r"C:\Coding\ACOS\test.txt"

        with open(email_file_path, "r", encoding="utf-8") as f:
            raw_email_body = f.read()

        apply_vendor_reply(raw_email_body)
        time.sleep(5)

    except Exception as e:
        import traceback
        print("ERROR:")
        time.sleep(5)
        traceback.print_exc()
        input("Press Enter to exit...")
