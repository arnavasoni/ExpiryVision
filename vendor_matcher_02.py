import pandas as pd
import re
from rapidfuzz import process, fuzz

# ---------------------------------------------------------
# PATHS
# ---------------------------------------------------------

BATCH_XLSX = r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DWT_ExpiryVision - Documents\Data\batch_details.xlsx"
VENDOR_MASTER_XLSX = r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DWT_ExpiryVision - Documents\Data\vendor_master.xlsx"

# ---------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------

batch_df = pd.read_excel(BATCH_XLSX, engine = "openpyxl")
vendor_master_df = pd.read_excel(VENDOR_MASTER_XLSX, engine = "openpyxl")

# ---------------------------------------------------------
# ENSURE REQUIRED COLUMNS
# ---------------------------------------------------------

for col in [
    "vendor_canonical_name",
    "vendor_email",
    "vendor_match_score",
    "vendor_match_status",
]:
    if col not in batch_df.columns:
        batch_df[col] = ""

# Safety: enforce uniqueness
if "batch_number" in batch_df.columns:
    batch_df = batch_df.drop_duplicates(subset=["batch_number"], keep="first")

# ---------------------------------------------------------
# NORMALIZATION
# ---------------------------------------------------------

LEGAL_SUFFIXES = [
    "ltd", "limited", "pvt", "pvt ltd", "private", "llp",
    "inc", "corp", "corporation", "company", "co",
    "gmbh", "ag", "sa", "bv", "nv",
    "india", "international"
]

def normalize_vendor_name(name: str) -> str:
    if not isinstance(name, str):
        return ""

    name = name.lower().strip()
    name = name.replace("&", "and")
    name = re.sub(r"[^\w\s]", " ", name)

    for suffix in LEGAL_SUFFIXES:
        name = re.sub(rf"\b{suffix}\b", "", name)

    name = re.sub(r"\s+", " ", name).strip()
    return name

vendor_master_df["normalized_name"] = vendor_master_df[
    "vendor_canonical_name"
].apply(normalize_vendor_name)

batch_df["normalized_vendor"] = batch_df[
    "vendor_or_brand"
].apply(normalize_vendor_name)

# ---------------------------------------------------------
# MATCHING FUNCTION
# ---------------------------------------------------------

def match_vendor(normalized_name, vendor_master_df):
    if not normalized_name:
        return "", "", 0, "UNMATCHED"

    choices = vendor_master_df["normalized_name"].tolist()

    result = process.extractOne(
        normalized_name,
        choices,
        scorer=fuzz.token_sort_ratio
    )

    if not result:
        return "", "", 0, "UNMATCHED"

    match, score, idx = result
    row = vendor_master_df.iloc[idx]

    if score >= 85:
        return row["vendor_canonical_name"], row["vendor_email"], score, "MATCHED"
    elif score >= 70:
        return row["vendor_canonical_name"], row["vendor_email"], score, "REVIEW"
    else:
        return "", "", score, "UNMATCHED"

# ---------------------------------------------------------
# APPLY MATCHING (ONLY WHERE EMPTY)
# ---------------------------------------------------------

mask = batch_df["vendor_canonical_name"].isna() | (batch_df["vendor_canonical_name"].astype(str).str.strip() == "")

results = batch_df.loc[mask, "normalized_vendor"].apply(
    lambda x: match_vendor(x, vendor_master_df)
)

batch_df.loc[mask, "vendor_canonical_name"] = results.apply(lambda x: x[0])
batch_df.loc[mask, "vendor_email"] = results.apply(lambda x: x[1])
batch_df.loc[mask, "vendor_match_score"] = results.apply(lambda x: x[2])
batch_df.loc[mask, "vendor_match_status"] = results.apply(lambda x: x[3])

# ---------------------------------------------------------
# SAVE BACK
# ---------------------------------------------------------

batch_df.to_excel(BATCH_XLSX, index=False, engine = "openpyxl")

print("Vendor matching completed")
print(batch_df["vendor_match_status"].value_counts())
