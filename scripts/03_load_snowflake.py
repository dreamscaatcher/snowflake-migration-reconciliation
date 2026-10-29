"""Phase 3: migrate legacy tables to Snowflake RAW (Parquet -> stage -> COPY INTO) and log every load."""
import os, sys, time
import duckdb, snowflake.connector

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
from config import SNOWFLAKE

WORK = os.path.join(os.path.expanduser("~"), "berlin_recon_work")
DB = os.environ.get("LEGACY_DB", os.path.join(WORK, "legacy.duckdb"))
TABLES = ["dim_property", "dim_unit", "fact_maintenance_ticket", "fact_service_charge_statement", "fact_rent_roll"]
SCHEMA = os.environ.get("TARGET_SCHEMA", "RAW")

def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)

TYPE_MAP = {"VARCHAR": "VARCHAR", "BOOLEAN": "BOOLEAN", "DATE": "DATE", "TIMESTAMP": "TIMESTAMP_NTZ(6)",
            "DOUBLE": "FLOAT", "BIGINT": "NUMBER(38,0)", "INTEGER": "NUMBER(38,0)", "SMALLINT": "NUMBER(38,0)"}

def create_ddl(src, t):
    """Explicit target DDL: UPPERCASE unquoted names, real types (no schema inference)."""
    cols = src.execute(f"select column_name, data_type from information_schema.columns where table_name='{t}' order by ordinal_position").fetchall()
    parts = []
    for name, typ in cols:
        sf = TYPE_MAP.get(typ)
        if sf is None and typ.startswith("DECIMAL"):
            sf = typ.replace("DECIMAL", "NUMBER")
        if sf is None:
            raise ValueError(f"unmapped type {typ} in {t}.{name}")
        parts.append(f"{name.upper()} {sf}")
    return f"create or replace table {t} ({', '.join(parts)})"

def main(only=None):
    src = duckdb.connect(DB, read_only=True)
    con = snowflake.connector.connect(**SNOWFLAKE)
    cur = con.cursor()
    cur.execute(f"create schema if not exists berlin_migration.{SCHEMA}")
    cur.execute(f"create schema if not exists berlin_migration.{SCHEMA}")
    cur.execute("create schema if not exists berlin_migration.recon")
    cur.execute(f"use schema berlin_migration.{SCHEMA}")
    cur.execute("""create table if not exists berlin_migration.recon.load_log (
        load_id number autoincrement, run_label string, table_name string, rows_source number,
        rows_loaded number, errors_seen number, seconds number(10,1), loaded_at timestamp_ntz default current_timestamp())""")
    cur.execute("create or replace file format parquet_ff type = parquet use_logical_type = true")
    cur.execute("create stage if not exists migration_stage file_format = parquet_ff")
    run_label = os.environ.get("RUN_LABEL", "run_A_clean")
    for t in (only or TABLES):
        t0 = time.time()
        n_src = src.execute(f"select count(*) from {t}").fetchone()[0]
        path = os.path.join(WORK, f"{t}.parquet")
        src.execute(f"copy {t} to '{path}' (format parquet, compression snappy)")
        log(f"{t}: exported {n_src:,} rows, {os.path.getsize(path)/1e6:.1f} MB")
        cur.execute(f"remove @migration_stage/{run_label}/{t}/")
        cur.execute(f"put file://{path} @migration_stage/{run_label}/{t}/ auto_compress=false overwrite=true")
        log(f"{t}: uploaded to stage")
        cur.execute(create_ddl(src, t))
        cur.execute(f"""copy into {t} from @migration_stage/{run_label}/{t}/ file_format = (format_name = 'parquet_ff')
            match_by_column_name = case_insensitive on_error = continue""")
        res = cur.fetchall()
        errors = sum(int(r[5]) for r in res)  # errors_seen column of COPY result
        n_tgt = cur.execute(f"select count(*) from {t}").fetchone()[0]
        secs = time.time() - t0
        cur.execute("insert into berlin_migration.recon.load_log (run_label, table_name, rows_source, rows_loaded, errors_seen, seconds) values (%s,%s,%s,%s,%s,%s)",
                    (run_label, t, n_src, n_tgt, errors, round(secs, 1)))
        log(f"{t}: loaded {n_tgt:,} rows, errors {errors}, {secs:.0f}s")
    con.close()
    log("DONE")

if __name__ == "__main__":
    main(sys.argv[1:] or None)
