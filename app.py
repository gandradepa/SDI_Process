import os
import sqlite3
from datetime import datetime
import subprocess

import pandas as pd
from flask import Flask, render_template, redirect, url_for, flash, request

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
def table_exists(conn, table_name):
    """Check if a table exists in the database."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cur.fetchone() is not None

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

def build_sdi_dataset(building_code: str = None) -> pd.DataFrame:
    """Mechanical + Electrical (Approved==1), harmonized to MASTER_COLS."""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found at: {DB_PATH}")
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        me = pd.read_sql_query("SELECT * FROM sdi_dataset;", conn)
        el = pd.read_sql_query("SELECT * FROM sdi_dataset_EL;", conn)

    me = filter_approved(me)
    el = filter_approved(el)
    
    if building_code:
        me = me[me['Building'].astype(str) == str(building_code)]
        el = el[el['Building'].astype(str) == str(building_code)]

    me = ensure_columns_and_order(me)

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

def get_all_buildings() -> list:
    """
    Fetches buildings that exist BOTH in the 'Buildings' table and have 
    corresponding assets in the sdi_dataset tables.
    """
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            if not table_exists(conn, 'Buildings'):
                flash("⚠️ 'Buildings' table not found. Using raw building codes for filtering.", "warning")
                df1 = pd.read_sql_query('SELECT DISTINCT Building FROM sdi_dataset', conn)
                df2 = pd.read_sql_query('SELECT DISTINCT Building FROM sdi_dataset_EL', conn)
                all_codes = pd.concat([df1, df2])['Building'].dropna().unique()
                all_codes.sort()
                return [{'Code': code, 'Name': f'Building {code}'} for code in all_codes]

            df1 = pd.read_sql_query('SELECT DISTINCT Building FROM sdi_dataset', conn)
            df2 = pd.read_sql_query('SELECT DISTINCT Building FROM sdi_dataset_EL', conn)
            asset_building_codes = set(pd.concat([df1, df2])['Building'].dropna().astype(str))

            df_buildings = pd.read_sql_query('SELECT Code, Name FROM Buildings', conn)
            df_buildings['Code'] = df_buildings['Code'].astype(str)

            df_filtered = df_buildings[df_buildings['Code'].isin(asset_building_codes)]
            
            df_filtered = df_filtered.sort_values('Name')
            return df_filtered.to_dict(orient="records")

    except Exception as e:
        error_msg = f"Could not generate building list: {e}"
        print(f"[Error] {error_msg}")
        flash(f"⚠️ {error_msg}", "danger")
        return []

def build_dashboard_dataset(building_code: str = None) -> pd.DataFrame:
    """Builds the main dataset, with an optional filter for building."""
    df = build_sdi_dataset(building_code=building_code).copy()
    
    df["QR Code"] = df["QR Code"].astype(str).str.strip()
    
    exported_codes = get_exported_codes_print_out_eq_1()
    if exported_codes:
        df = df[~df["QR Code"].isin(exported_codes)].copy()

    try:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            if table_exists(conn, 'Buildings'):
                df_buildings = pd.read_sql_query('SELECT Code, Name FROM Buildings', conn)
                df_buildings['Code'] = df_buildings['Code'].astype(str)
                
                df['Building'] = df['Building'].astype(str)
                df = pd.merge(df, df_buildings, left_on='Building', right_on='Code', how='left')
                df.drop(columns=['Building', 'Code'], inplace=True)
                df.rename(columns={'Name': 'Building'}, inplace=True)
                df['Building'].fillna('Unknown Building', inplace=True)
    except Exception as e:
        print(f"[Warning] Could not merge building names: {e}")

    return ensure_columns_and_order(df)


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
                "QR Code" TEXT, "Building" TEXT, "Description" TEXT, 
                "Asset Group" TEXT, "UBC Tag" TEXT, "Serial" TEXT, "Model" TEXT, 
                "Manufacturer" TEXT, "Attribute" TEXT, "Ampere" TEXT, 
                "Supply From" TEXT, "Volts" TEXT, "Location" TEXT, "Diameter" TEXT, 
                "Technical Safety BC" TEXT, "Year" TEXT, "print_out" INTEGER, 
                "date" TEXT, "time" TEXT
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
    selected_building_code = request.args.get("building_code", "")
    all_buildings = get_all_buildings()
    df = build_dashboard_dataset(building_code=selected_building_code)
    
    return render_template(
        "dashboard.html",
        title="List of Assets Ready to be Loaded to Planon",
        columns=MASTER_COLS,
        rows=df.to_dict(orient="records"),
        logo_main_name=LOGO_MAIN_NAME,
        logo_fac_name=LOGO_FAC_NAME,
        all_buildings=all_buildings,
        selected_building=selected_building_code
    )

@app.route("/export", methods=["POST"])
def export_to_sdi():
    building_code = request.form.get("building_code")

    if not building_code:
        flash("To create a pack, select only one building at time", "warning")
        return redirect(url_for("dashboard"))

    try:
        df = build_sdi_dataset(building_code=building_code)

        if df.empty:
            flash(f"No assets to export for the selected building.", "info")
            return redirect(url_for("dashboard", building_code=building_code))

        df_print = build_print_out_frame(df)
        n = export_full_print_out(df_print)
        flash(f"✅ Exported {n} rows for the selected building to SDI successfully.", "success")
    except Exception as e:
        print("[Export Error]", repr(e))
        flash(f"⚠️ Could not record the export. {str(e)}", "danger")
    
    return redirect(url_for("dashboard", building_code=building_code))

@app.route("/export-planon", methods=["POST"])
def export_to_planon():
    """Executes the SDI_Spreadsheet.py script."""
    try:
        script_path = os.path.join(BASE_DIR, "SDI_Spreadsheet.py")
        if not os.path.exists(script_path):
            flash(f"⚠️ Script not found at {script_path}", "danger")
            return redirect(url_for("dashboard"))

        # Executa o script
        result = subprocess.run(["python", script_path], capture_output=True, text=True, check=True)
        
        print("Script output:", result.stdout)
        flash("✅ 'Export to Planon' script executed successfully!", "success")

    except FileNotFoundError:
        flash(f"⚠️ Could not find the Python interpreter. Make sure Python is in your system's PATH.", "danger")
    except subprocess.CalledProcessError as e:
        print("[Script Execution Error]", e.stderr)
        flash(f"⚠️ Error executing the script: {e.stderr}", "danger")
    except Exception as e:
        print("[Export Planon Error]", repr(e))
        flash(f"⚠️ An unexpected error occurred: {str(e)}", "danger")
        
    return redirect(url_for("dashboard"))

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8_003, debug=True)

