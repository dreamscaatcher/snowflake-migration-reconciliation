"""Phase 1b: profile every legacy table column by column. This is the 'before' picture."""
import os, csv, duckdb

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.environ.get("LEGACY_DB", os.path.join(os.path.expanduser("~"), "berlin_recon_work", "legacy.duckdb"))
OUT = os.path.join(HERE, "docs", "baseline_profile.csv")
NUM = ("INTEGER", "BIGINT", "DOUBLE", "DECIMAL", "HUGEINT", "FLOAT", "SMALLINT")
con = duckdb.connect(DB, read_only=True)
rows = []
for (t,) in con.execute("select table_name from information_schema.tables where table_schema='main' order by 1").fetchall():
    n = con.execute(f'select count(*) from "{t}"').fetchone()[0]
    for col, typ in con.execute(f"select column_name, data_type from information_schema.columns where table_name='{t}' order by ordinal_position").fetchall():
        c = f'"{col}"'
        nulls, distinct = con.execute(f"select count(*) filter (where {c} is null), count(distinct {c}) from \"{t}\"").fetchone()
        mn = mx = sm = None
        if typ.startswith(NUM) or typ.startswith("TIMESTAMP") or typ == "DATE":
            mn, mx = con.execute(f'select min({c}), max({c}) from "{t}"').fetchone()
        if typ.startswith(NUM):
            sm = con.execute(f'select sum({c}) from "{t}"').fetchone()[0]
        rows.append(dict(table=t, rows=n, column=col, type=typ, nulls=nulls, distinct=distinct, min=mn, max=mx, sum=sm))
with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader(); w.writerows(rows)
print("columns profiled:", len(rows), "->", OUT)
