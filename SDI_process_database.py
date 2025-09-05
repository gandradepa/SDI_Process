import os
import sqlite3
import pandas as pd

# -------------------------------------------------------------------
# Path to the SQLite database (Windows network drive S:)
# -------------------------------------------------------------------
DB_PATH = r"S:\MaintOpsPlan\AssetMgt\Asset Management Process\Database\8. New Assets\Git_control\SDI Process\QR_codes.db"

# -------------------------------------------------------------------
# Master column order/schema (will be enforced on both datasets)
# -------------------------------------------------------------------
MASTER_COLS = [
    "QR Code", "Building", "Description", "Asset Group",
    "UBC Tag", "Serial", "Model", "Manufacturer", "Attribute",
    "Ampere", "Supply From", "Volts", "Location",
    "Diameter", "Technical Safety BC", "Year"
]

def ensure_columns_and_order(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Add any missing columns with blank values and return df in the exact order."""
    missing = [c for c in cols if c not in df.columns]
    for c in missing:
        df[c] = ""  # blank values for missing columns
    return df[cols]  # enforce order

def filter_approved(df: pd.DataFrame) -> pd.DataFrame:
    """Filter rows where Approved == 1, supporting numeric or text."""
    if "Approved" not in df.columns:
        return df
    if df["Approved"].dtype == "object":
        return df[df["Approved"] == "1"]
    else:
        return df[df["Approved"] == 1]

# -------------------------------------------------------------------
# Safety check
# -------------------------------------------------------------------
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"Database not found at: {DB_PATH}")

# -------------------------------------------------------------------
# Connect to SQLite and read both tables
# -------------------------------------------------------------------
with sqlite3.connect(DB_PATH) as conn:
    Mechanical = pd.read_sql_query("SELECT * FROM sdi_dataset;", conn)
    Electrical = pd.read_sql_query("SELECT * FROM sdi_dataset_EL;", conn)

print("✅ Loaded tables")
print("Mechanical shape:", Mechanical.shape)
print("Electrical shape:", Electrical.shape)

# -------------------------------------------------------------------
# Filter Approved == 1
# -------------------------------------------------------------------
Mechanical = filter_approved(Mechanical)
Electrical = filter_approved(Electrical)

# -------------------------------------------------------------------
# Electrical: rename "UBC Asset Tag" -> "UBC Tag"
# -------------------------------------------------------------------
if "UBC Asset Tag" in Electrical.columns and "UBC Tag" not in Electrical.columns:
    Electrical = Electrical.rename(columns={"UBC Asset Tag": "UBC Tag"})

# -------------------------------------------------------------------
# Normalize schemas
# -------------------------------------------------------------------
Mechanical = ensure_columns_and_order(Mechanical, MASTER_COLS)
Electrical = ensure_columns_and_order(Electrical, MASTER_COLS)

# -------------------------------------------------------------------
# Join vertically -> new dataset: sdi_dataset
# -------------------------------------------------------------------
sdi_dataset = pd.concat([Mechanical, Electrical], ignore_index=True)

print("✅ Combined Mechanical + Electrical → sdi_dataset")
print("Rows:", len(sdi_dataset), "| Columns:", list(sdi_dataset.columns))
print(sdi_dataset.dtypes)
