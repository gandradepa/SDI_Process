import os
import sqlite3
from datetime import datetime

import pandas as pd
from flask import Flask, render_template, redirect, url_for, flash

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "template")
STATIC_DIR   = os.path.join(BASE_DIR, "static")
DB_PATH = r"S:\MaintOpsPlan\AssetMgt\Asset Management Process\Database\8. New Assets\Git_control\SDI Process\QR_codes.db"

LOGO_MAIN_NAME = "ubc_logo.jpg"
LOGO_FAC_NAME  = "ubc-facilities_logo.jpg"

# -----------------------------------------------------------------------------
# Flask
# -----------------------------------------------------------------------------
app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR, static_url_path="/static")
app.secret_key = "replace-with-a-strong-secret"

# -----------------------------------------------------------------------------
# Columns
# -----------------------------------------------------------------------------
MASTER_COLS = [
    "QR Code","Building","Description","Asset Group","UBC Tag","Serial","Model",
    "Manufacturer","Attribute","Ampere","Supply From","Volts","Location",
    "Diameter","Technical Safety BC","Year"
]

PRINT_OUT_COLS = [
    "QR Code","Building","Description","Asset Group","UBC Tag","Serial","Model",
    "Manufacturer","Attribute","Ampere","Supply From","Volts","Location",
    "Diameter","Technical Safety BC","Year","print_out","date","time"
]

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def ensure_columns_and_order(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in MASTER_COLS:
        if c not in df.columns:
            df[c] = ""
    return df.loc[:, MASTER_COLS]

def filter_approved(df: pd.DataFrame) -> pd.DataFrame:
    if "Approved" not in df.columns:
        return df.copy()
    return df.loc[df["Approved"].astype(str) == "1"].copy()

def build_sdi_dataset() -> pd.DataFrame:
    """Mechanical + Electrical (Approved==1), harmonized to MASTER_COLS."""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found at: {DB_PATH}")
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        me = pd.read_sql_query("SELECT * FROM sdi_dataset;", conn)
        el = pd.read_sql_query("SELECT * FROM sdi_dataset_EL;", conn)

    me = ensure_columns_and_order(filter_approved(me))

    el = filter_approved(el)
    if "UBC Asset Tag" in el.columns and "UBC Tag" not in el.columns:
        el = el.rename(columns={"UBC Asset Tag": "UBC Tag"})
    el = ensure_columns_and_order(el)

    return pd.concat([me, el], ignore_index=True)

def get_exported_codes_print_out_eq_1() -> set:
    """Return 'QR Code' values in sdi_print_out with print_out == 1."""
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            df_exp = pd.read_sql_query('SELECT "QR Code", print_out FROM sdi_print_out', conn)
    except Exception:
        return set()

    if "QR Code" not in df_exp.columns or "print_out" not in df_exp.columns:
        return set()

    df_exp["QR Code"] = df_exp["QR Code"].astype(str).str.strip()
    po = df_exp["print_out"].astype(str).str.strip()
    df_exp = df_exp.loc[po == "1"]
    return set(df_exp["QR Code"].tolist())

def build_dashboard_dataset() -> pd.DataFrame:
    df = build_sdi_dataset().copy()
    df["QR Code"] = df["QR Code"].astype(str).str.strip()
    exported_codes = get_exported_codes_print_out_eq_1()
    if exported_codes:
        df = df[~df["QR Code"].isin(exported_codes)].copy()
    return df

def build_print_out_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Convert dashboard DF to sdi_print_out schema + add print_out/date/time."""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")

    out = df.copy()
    for c in PRINT_OUT_COLS:
        if c not in out.columns:
            out[c] = ""

    out["print_out"] = 0
    out["date"] = date_str
    out["time"] = time_str
    return out.loc[:, PRINT_OUT_COLS]

def _check_db_writable(path: str):
    folder = os.path.dirname(path) or "."
    if not os.access(folder, os.W_OK):
        raise PermissionError(f"Folder not writable: {folder}")
    if os.path.exists(path) and not os.access(path, os.W_OK):
        raise PermissionError(f"Database file is read-only: {path}")

def export_full_print_out(df_print: pd.DataFrame) -> int:
    """Replace rows in sdi_print_out with df_print."""
    _check_db_writable(DB_PATH)
    with sqlite3.connect(DB_PATH, timeout=20) as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sdi_print_out (
                "QR Code" TEXT,
                "Building" TEXT,
                "Description" TEXT,
                "Asset Group" TEXT,
                "UBC Tag" TEXT,
                "Serial" TEXT,
                "Model" TEXT,
                "Manufacturer" TEXT,
                "Attribute" TEXT,
                "Ampere" TEXT,
                "Supply From" TEXT,
                "Volts" TEXT,
                "Location" TEXT,
                "Diameter" TEXT,
                "Technical Safety BC" TEXT,
                "Year" TEXT,
                "print_out" INTEGER,
                "date" TEXT,
                "time" TEXT
            )
        """)
        cur.execute("DELETE FROM sdi_print_out")
        conn.commit()
        df_print.to_sql("sdi_print_out", conn, if_exists="append", index=False, method="multi", chunksize=500)
        conn.commit()
        return len(df_print)

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/")
def dashboard():
    df = build_dashboard_dataset()
    return render_template(
        "dashboard.html",
        title="List of Assets Ready to be Loaded to Planon",
        columns=MASTER_COLS,
        rows=df.to_dict(orient="records"),
        logo_main_name=LOGO_MAIN_NAME,
        logo_fac_name=LOGO_FAC_NAME
    )

@app.route("/export", methods=["POST"])
def export_to_sdi():
    try:
        df = build_sdi_dataset()
        df_print = build_print_out_frame(df)
        n = export_full_print_out(df_print)
        flash(f"✅ Exported {n} rows to SDI successfully.", "success")
    except Exception as e:
        print("[Export Error]", repr(e))
        flash(f"⚠️ Could not record the export. {str(e)}", "danger")
    return redirect(url_for("dashboard"))

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8003, debug=True)
