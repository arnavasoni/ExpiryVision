import pandas as pd
from datetime import datetime
import win32com.client as win32

CSV_PATH = r"C:\Coding\ACOS\data\batch_details.csv"

ALERT_DAYS = 20
CRITICAL_DAYS = 10

def calculate_status(expiry_date_str):
    if not expiry_date_str or pd.isna(expiry_date_str):
        return "", ""

    expiry_date = None

    for fmt in ("%Y-%m-%d", "%d-%m-%y", "%d-%m-%Y"):
        try:
            expiry_date = datetime.strptime(expiry_date_str, fmt).date()
            break
        except ValueError:
            continue

    if not expiry_date:
        return "", ""

    today = datetime.today().date()
    days_left = (expiry_date - today).days

    if days_left < 0:
        status = "CRITICAL"
    elif days_left < 10:
        status = "CRITICAL"
    elif days_left <= 20:
        status = "ALERT"
    else:
        status = "NORMAL"

    return status, days_left


def update_status_in_csv(csv_path=CSV_PATH):
    df = pd.read_csv(csv_path)

    if "status" not in df.columns:
        df["status"] = ""

    if "days_pending" not in df.columns:
        df["days_pending"] = ""

    df["status"], df["days_pending"] = zip(*df["expiry_date"].apply(calculate_status))

    df.to_csv(csv_path, index=False)
    return df

def build_email_table(df):
    alert_df = df[df["status"].isin(["CRITICAL", "ALERT"])]

    if alert_df.empty:
        return ""

    columns = [
        "batch_number",
        "expiry_date",
        "units_in_batch",
        "quantity_per_unit",
        "total_quantity",
        "UOM",
        "vendor_or_brand",
        "status",
        "days_pending",
    ]

    alert_df = alert_df[columns]

    return alert_df.to_html(
        index=False,
        border=1,
        justify="center",
        classes="expiry-table"
    )

def send_outlook_email(html_table):
    if not html_table:
        print("No ALERT or CRITICAL items. Email not sent.")
        return

    outlook = win32.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)

    mail.Subject = "⚠ Batch Expiry Alert"
    # mail.To = "your_email@company.com"  # CHANGE THIS
    mail.To = "arnava.soni@mercedes-benz.com"

    mail.HTMLBody = f"""
    <html>
    <body>
        <p>Dear Team,</p>

        <p>The following batches are nearing expiry and require attention:</p>

        {html_table}

        <p>
        <b>Status Legend:</b><br>
        CRITICAL: &lt; 10 days to expiry<br>
        ALERT: ≤ 20 days to expiry
        </p>

        <p>Regards,<br>
        Automated Expiry Monitoring System</p>
    </body>
    </html>
    """

    mail.Send()
    print("Expiry alert email sent.")

if __name__ == "__main__":
    df = update_status_in_csv()
    table_html = build_email_table(df)
    send_outlook_email(table_html)
