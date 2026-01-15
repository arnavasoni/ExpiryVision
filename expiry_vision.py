# dependencies

# UN notes as optional for DG goods category

# Provides classes for integrating Gemini models to the LangChain framework.
from langchain_google_genai import ChatGoogleGenerativeAI
# A method to convert images, files into a text-only format.
import base64
# Helps to interact with the underlying operating system.
import os
# To load local environment variables from a .env file.
from dotenv import load_dotenv
import json
# Messages are objects used in prompts and chat conversations.
from langchain_core.messages import HumanMessage
# Pydantic is a data validation and settings management library.
from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime
import pandas as pd


# ---------------------------------------------------------
# 1. PYDANTIC MODEL FOR STRUCTURED OUTPUT
# Created a class with attributes to store extracted data. They are Model Fields.
# ---------------------------------------------------------

class LabelExtractionResult(BaseModel):
    product_description: str # str is a type annotation, not an assignment. This attribute exists and the type should be a string.
    vendor_or_brand: str
    batch_number: str
    expiry_date: str   # YYYY-MM-DD
    units_in_batch: int
    quantity_per_unit: int
    UOM: str
    mode_of_transport: str       # air, ship, road
    UN_Number: Optional[str] # A 6-character, alphanumeric code used to identify dangerous goods and how should they be stored.

    # Above are instance attributes. Each instance has its own values.

    # ----- VALIDATORS -----


    @field_validator("expiry_date") # decorator. tells pydantic to use this whenever Expiry_Date field is set.
    def validate_expiry_format(cls, v): # cls: Class LabelExtractionResult; v: Incoming value for Expiry_Date
        if not v:
            return ""
        try:
            dt = datetime.strptime(v.replace("/", "-"), "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except:
            return ""   # fallback – ensures no crash

    # Before Pydantic validates Units_in_Batch and Quantity_per_Unit, it calls this function to try converting the incoming values to integer.
    @field_validator("units_in_batch", "quantity_per_unit", mode="before")
    def int_fields(cls, v):
        try:
            return int(v)
        except:
            return 0


    @field_validator("mode_of_transport")
    def validate_transport(cls, v):
        allowed = ["air", "ship", "road", ""]
        v = v.lower().strip()
        return v if v in allowed else ""


# ---------------------------------------------------------
# 2. GEMINI CLIENT INITIALIZATION
# ---------------------------------------------------------

load_dotenv()
nexus_api_key = os.getenv("NEXUS_API_KEY")
nexus_base_url = os.getenv("NEXUS_BASE_URL")

# Initialize the client (configuration)
client = ChatGoogleGenerativeAI(
    model="gemini-2.5-pro",
    google_api_key=nexus_api_key,
    client_options={"api_endpoint": nexus_base_url}, # overrides the default Google endpoint cuz Gateway needed.
    transport="rest", # requests are sent as HTTP requests. This is how we talk to the server.
    temperature=0, # deterministic output. Same input -> same output
    max_output_tokens=1024, # to cap response size. Tokens are similar to chunks of words (here).
)


# ---------------------------------------------------------
# 3. IMAGE UTILITY FUNCTIONS
# ---------------------------------------------------------

def get_mime_type(ext):
    ext = ext.lower()
    if ext in [".jpg", ".jpeg"]:
        return "image/jpeg"
    elif ext == ".png":
        return "image/png"
    return "application/octet-stream"


# ---------------------------------------------------------
# 4. MAIN IMAGE PROCESSOR
# ---------------------------------------------------------

def process_image_with_gemini(image_path):
    """Process image using Gemini Pro Vision LLM — returns Pydantic validated output."""


    # Image → Base64
    try:
        with open(image_path, "rb") as img_file:
            b64 = base64.b64encode(img_file.read()).decode("utf-8")
    except Exception as e:
        return {"error": f"Could not read image: {str(e)}"}


    mime = get_mime_type(os.path.splitext(image_path)[1])
    data_url = f"data:{mime};base64,{b64}"


    # Prompt instructing exact JSON schema
    prompt_text = """
        Extract structured data from the image of a box label.


        Return ONLY a VALID JSON object with EXACT KEYS:


        - product_description (string)
        - vendor_or_brand (string)
        - batch_number (string)
        - expiry_date (string, YYYY-MM-DD)
        - units_in_batch (integer)
        - quantity_per_unit (integer)
        - UOM (string)
        - mode_of_transport (string: "air", "ship", "road")
        - UN_Number (string: 6 characters, usually starts with 'UN'. Eg: "UN1866")


        RULES:
        1. Output ONLY a JSON object, no explanations.
        2. UOM means Unit Of Measurement.
        3. Use empty strings if missing.
        4. Ensure JSON is strictly valid.
        """


    msg = HumanMessage(
        content=[
            {"type": "text", "text": prompt_text},
            {"type": "image_url", "image_url": {"url": data_url}},
        ]
    )


    try:
        structured_client = client.with_structured_output(LabelExtractionResult)
        result = structured_client.invoke([msg])
        return result.model_dump()

    except Exception as e:
        return {"error": f"LLM processing failed: {str(e)}"}

# ---------------------------------------------------------
# 5. CSV UPDATER
# ---------------------------------------------------------

CSV_PATH = r"C:\Coding\ACOS\data\batch_details.csv"

def update_csv_with_extraction(result: dict, csv_path: str = CSV_PATH):
    """
    Updates batch_details.csv using Gemini output.
    - Matches rows using batch_number
    - Updates only empty cells
    - Never deletes or overwrites existing data
    """

    if "error" in result:
        print("Skipping CSV update due to extraction error.")
        return

    # Normalize Gemini keys → CSV keys
    extracted = {
        "batch_number": result.get("batch_number", "").strip(),
        "expiry_date": result.get("expiry_date", ""),
        "units_in_batch": result.get("units_in_batch", 0),
        "quantity_per_unit": result.get("quantity_per_unit", 0),
        "UOM": result.get("UOM", ""),
        "vendor_or_brand": result.get("vendor_or_brand", ""),
}


    if not extracted["batch_number"]:
        print("Batch number missing. Skipping CSV update.")
        return

    # Load CSV
    df = pd.read_csv(csv_path)

    # Ensure expected columns exist
    expected_columns = [
        "part_number",
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

    for col in expected_columns:
        if col not in df.columns:
            df[col] = ""

    # Check if batch already exists
    match_idx = df.index[df["batch_number"] == extracted["batch_number"]].tolist()

    if match_idx:
        idx = match_idx[0]
        row = df.loc[idx]

        # Update only empty fields
        for col, value in extracted.items():
            if (pd.isna(row[col]) or str(row[col]).strip() == "") and value not in ["", 0]:
                df.at[idx, col] = value

        # Calculate total quantity ONLY if empty
        if (
            (pd.isna(row["total_quantity"]) or str(row["total_quantity"]).strip() == "")
            and extracted["units_in_batch"] > 0
            and extracted["quantity_per_unit"] > 0
        ):
            df.at[idx, "total_quantity"] = (
                extracted["units_in_batch"] * extracted["quantity_per_unit"]
            )

    else:
        # Create new row
        total_qty = (
            extracted["units_in_batch"] * extracted["quantity_per_unit"]
            if extracted["units_in_batch"] > 0 and extracted["quantity_per_unit"] > 0
            else ""
        )

        new_row = {
            "part_number": "",
            "batch_number": extracted["batch_number"],
            "expiry_date": extracted["expiry_date"],
            "units_in_batch": extracted["units_in_batch"],
            "quantity_per_unit": extracted["quantity_per_unit"],
            "total_quantity": total_qty,
            "UOM": extracted["UOM"],
            "vendor_or_brand": extracted["vendor_or_brand"],
        }

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    # Write back safely
    df.to_csv(csv_path, index=False)
    print(f"CSV updated for batch: {extracted['batch_number']}")


# ---------------------------------------------------------
# 5. TESTING
# ---------------------------------------------------------


if __name__ == "__main__":
    paths = [
        # r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\eftec_label.jpg",
        # r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\eftec_label_2.jpg",
        # r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\sika_label_1.jpg",
        # r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\sika_label_2.jpg",
        # r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\Image_1.jpg",
        r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\Image_2.jpg",
        r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\Image_3.jpg",
        r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\Image_6.jpg",
        # r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\Image_7.jpg",
        # r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\Image_8.jpg",
        # r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DISCO\Database\label_images\Image_9.jpg",
    ]


    for p in paths:
        print(f"\n=== Processing: {p} ===")
        result = process_image_with_gemini(p)
        print(json.dumps(result, indent=2))
        update_csv_with_extraction(result)
