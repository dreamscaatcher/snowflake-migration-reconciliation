"""Phase 1: load the Excel workbook (the 'legacy system') into a local DuckDB file, untouched."""
import os, time
import pandas as pd, duckdb
from openpyxl import load_workbook

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XLSX = os.environ.get("XLSX_PATH", os.path.join(os.path.dirname(HERE), "Berlin_Property_Ops_33k.xlsx"))
DB = os.environ.get("LEGACY_DB", os.path.join(os.path.expanduser("~"), "berlin_recon_work", "legacy.duckdb"))

# target table -> source sheets (rent roll is split in two sheets and re-unioned)
TABLES = {
    "dim_property": ["dim_property"],
    "dim_unit": ["dim_unit"],
    "fact_rent_roll": ["fact_rent_roll_2024", "fact_rent_roll_2025_2026"],
    "fact_maintenance_ticket": ["fact_maintenance_ticket"],
    "fact_service_charge_statement": ["fact_service_charge_statement"],
}

def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)

def sheet_df(wb, name):
    rows = list(wb[name].values)
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df = df.replace("", None)
    for c in df.columns:  # Excel stores whole numbers as floats: restore integers
        if df[c].dtype == "float64":
            s = df[c].dropna()
            if len(s) and (s == s.round()).all():
                df[c] = df[c].astype("Int64")
    return df

def main():
    wb = load_workbook(XLSX, read_only=True, data_only=True)
    if os.path.exists(DB):
        os.remove(DB)
    con = duckdb.connect(DB)
    for table, sheets in TABLES.items():
        parts = []
        for s in sheets:
            t0 = time.time()
            df = sheet_df(wb, s)
            log(f"read sheet {s}: {len(df):,} rows in {time.time()-t0:.0f}s")
            parts.append(df)
        df = pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0]
        con.register("df_tmp", df)
        con.execute(f"create table {table} as select * from df_tmp")
        con.unregister("df_tmp")
        n = con.execute(f"select count(*) from {table}").fetchone()[0]
        log(f"TABLE {table}: {n:,} rows loaded")
    con.close()
    log("DONE legacy.duckdb")

if __name__ == "__main__":
    main()
