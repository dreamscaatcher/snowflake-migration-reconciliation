"""Phase 5: reconcile legacy (DuckDB) against Snowflake. Writes PASS/FAIL rows to RECON.RESULTS.
Usage: RUN_LABEL=run_A_clean TARGET_SCHEMA=RAW python scripts/04_reconcile.py
"""
import os, sys, time, collections
import duckdb, snowflake.connector

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
from config import SNOWFLAKE

DB = os.environ.get("LEGACY_DB", os.path.join(os.path.expanduser("~"), "berlin_recon_work", "legacy.duckdb"))
SCHEMA = os.environ.get("TARGET_SCHEMA", "RAW")
RUN = os.environ.get("RUN_LABEL", "run_A_clean")

TABLES = ["dim_property", "dim_unit", "fact_rent_roll", "fact_maintenance_ticket", "fact_service_charge_statement"]
KEYS = {"dim_property": ["property_id"], "dim_unit": ["unit_id"], "fact_rent_roll": ["unit_id", "period_month"],
        "fact_maintenance_ticket": ["ticket_id"], "fact_service_charge_statement": ["statement_id"]}
MONEY = {"fact_rent_roll": ["kaltmiete_due", "nk_prepayment_due", "total_due", "paid_by_due_date", "paid_by_month_end", "paid_to_snapshot"],
         "fact_maintenance_ticket": ["cost_eur"],
         "fact_service_charge_statement": ["prepayments_total", "actual_costs_total", "balance_eur", "forfeited_backpayment_eur"]}
ORPHANS = [("fact_maintenance_ticket", "property_id", "dim_property", "property_id"),
           ("fact_maintenance_ticket", "unit_id", "dim_unit", "unit_id"),
           ("fact_rent_roll", "unit_id", "dim_unit", "unit_id"),
           ("fact_service_charge_statement", "unit_id", "dim_unit", "unit_id"),
           ("dim_unit", "property_id", "dim_property", "property_id")]
KPIS = [  # name, table, sql expression (valid in DuckDB and Snowflake), tolerance
    ("Soll total (EUR)", "fact_rent_roll", "sum(total_due)", 0.01),
    ("Collection Rate In Month %", "fact_rent_roll", "sum(paid_by_month_end) / sum(total_due)", 1e-9),
    ("Open Arrears at Snapshot (EUR)", "fact_rent_roll",
     "sum(case when due_date <= date '2026-09-30' and due_date > date '2025-09-30' then greatest(0, total_due - paid_to_snapshot) end)", 0.01),
    ("Resolution SLA %", "fact_maintenance_ticket", "cast(count_if(met_resolution_sla) as double) / count(met_resolution_sla)", 1e-9),
    ("Response SLA %", "fact_maintenance_ticket", "cast(count_if(met_response_sla) as double) / count(met_response_sla)", 1e-9),
    ("Open Tickets", "fact_maintenance_ticket", "count_if(is_open)", 0),
    ("Median Resolution Hours", "fact_maintenance_ticket", "median(resolution_hours)", 1e-9),
    ("Statements On Time %", "fact_service_charge_statement",
     "cast(count_if(timeliness = 'Fristgerecht') as double) / count_if(timeliness in ('Fristgerecht', 'Verspätet'))", 1e-9),
    ("Forfeited Back-payments (EUR)", "fact_service_charge_statement", "sum(forfeited_backpayment_eur)", 0.01),
    ("Avg Days To Issue", "fact_service_charge_statement", "avg(days_to_issue)", 1e-9),
]
CAUSE = {"R01": "rows lost or loaded twice", "R02": "rounding, truncation or lost rows", "R03": "double load; Snowflake does not enforce keys",
         "R04": "type conversion or empty-string vs NULL handling", "R05": "timezone shift or date cut-off",
         "R06": "key case sensitivity or missing parent rows", "R07": "values changed in transit (see detail)",
         "R08": "KPI differs: logic, data, or SQL dialect (e.g. integer division precision)"}

src = duckdb.connect(DB, read_only=True)
sf = snowflake.connector.connect(**SNOWFLAKE)
cur = sf.cursor()
cur.execute(f"use schema berlin_migration.{SCHEMA}")
results = []

def S(sql):  # source query
    return src.execute(sql).fetchall()

def T(sql):  # target query
    cur.execute(sql); return cur.fetchall()

def add(cid, name, table, metric, s, t, tol=0.0, detail="", numeric=True):
    if numeric:
        s_, t_ = (None if s is None else float(s)), (None if t is None else float(t))
        diff = None if (s_ is None or t_ is None) else abs(s_ - t_)
        ok = (s_ is None and t_ is None) or (diff is not None and diff <= tol)
    else:
        diff = None; ok = str(s) == str(t)
    res = "PASS" if ok else "FAIL"
    results.append((RUN, cid, name, table, metric, str(s), str(t), diff, tol, res,
                    detail if ok or detail else CAUSE[cid], ))
    if not ok and not detail:
        results[-1] = results[-1][:-1] + (CAUSE[cid],)

def cols(t):
    return src.execute(f"select column_name, data_type from information_schema.columns where table_name='{t}' order by ordinal_position").fetchall()

# ---------- normalised value expressions (identical text on both sides) ----------
def norm_src(c, typ):
    if typ == "VARCHAR": e = c
    elif typ == "TIMESTAMP": e = f"strftime({c}, '%Y-%m-%d %H:%M:%S')"
    elif typ == "DOUBLE": e = f"cast(cast({c} as decimal(18,4)) as varchar)"
    else: e = f"cast({c} as varchar)"
    return f"coalesce({e}, '<null>')"

def norm_tgt(c, typ):
    if typ == "VARCHAR": e = c
    elif typ == "TIMESTAMP": e = f"to_char({c}, 'YYYY-MM-DD HH24:MI:SS')"
    elif typ == "DOUBLE": e = f"to_varchar({c}::number(18,4))"
    else: e = f"{c}::varchar"
    return f"coalesce({e}, '<null>')"

def log(m): print(time.strftime("%H:%M:%S"), m, flush=True)

# ---------- R01 row counts, R02 sums, R03 keys ----------
for t in TABLES:
    add("R01", "Row count", t, "count(*)", S(f"select count(*) from {t}")[0][0], T(f"select count(*) from {t}")[0][0])
    for c in MONEY.get(t, []):
        add("R02", "Sum of money column", t, f"sum({c})", S(f"select sum({c}) from {t}")[0][0], T(f"select sum({c}) from {t}")[0][0], tol=0.01)
    kcols = KEYS[t]
    ks = "concat_ws('|', " + ", ".join(f"cast({k} as varchar)" for k in kcols) + ")"
    kt = "concat_ws('|', " + ", ".join(f"{k}::varchar" for k in kcols) + ")"
    sd, sn = S(f"select count(distinct {ks}), count(*) from {t}")[0]
    td, tn = T(f"select count(distinct {kt}), count(*) from {t}")[0]
    add("R03", "Distinct keys", t, "distinct keys " + "+".join(kcols), sd, td)
    add("R03", "Duplicate key rows", t, "rows minus distinct keys", sn - sd, tn - td)
log("R01-R03 done")

# ---------- R04 null counts, R05 date ranges ----------
for t in TABLES:
    cs = cols(t)
    qs = ", ".join(f"count(*) filter (where {c} is null)" for c, _ in cs)
    qt = ", ".join(f"count_if({c} is null)" for c, _ in cs)
    rs, rt = S(f"select {qs} from {t}")[0], T(f"select {qt} from {t}")[0]
    bad = [f"{c}: {a} vs {b}" for (c, _), a, b in zip(cs, rs, rt) if a != b]
    add("R04", "Null counts per column", t, f"{len(cs)} columns", sum(rs), sum(rt), tol=0,
        detail="; ".join(bad[:6]) if bad else "all columns equal")
    if bad:  # force FAIL even when totals happen to cancel out
        results[-1] = results[-1][:9] + ("FAIL",) + results[-1][10:]
    dcols = [(c, ty) for c, ty in cs if ty in ("DATE", "TIMESTAMP")]
    if dcols:
        fmt_s = lambda c, ty: f"cast(min({c}) as varchar) || ' / ' || cast(max({c}) as varchar)" if ty == "DATE" else \
            f"strftime(min({c}), '%Y-%m-%d %H:%M:%S') || ' / ' || strftime(max({c}), '%Y-%m-%d %H:%M:%S')"
        fmt_t = lambda c, ty: f"min({c})::varchar || ' / ' || max({c})::varchar" if ty == "DATE" else \
            f"to_char(min({c}), 'YYYY-MM-DD HH24:MI:SS') || ' / ' || to_char(max({c}), 'YYYY-MM-DD HH24:MI:SS')"
        rs = S("select " + ", ".join(fmt_s(c, ty) for c, ty in dcols) + f" from {t}")[0]
        rt = T("select " + ", ".join(fmt_t(c, ty) for c, ty in dcols) + f" from {t}")[0]
        for (c, _), a, b in zip(dcols, rs, rt):
            add("R05", "Date range (min / max)", t, c, a, b, numeric=False, detail="" if a == b else f"source {a} vs target {b}")
log("R04-R05 done")

# ---------- R06 orphans ----------
for ct, cc, pt, pc in ORPHANS:
    q = f"select count(*) from {ct} c where c.{cc} is not null and not exists (select 1 from {pt} p where p.{pc} = c.{cc})"
    add("R06", "Orphan keys", ct, f"{ct}.{cc} -> {pt}.{pc}", S(q)[0][0], T(q)[0][0])
log("R06 done")

# ---------- R07 row-level hashes (bucketed fingerprint, drill-down on mismatch) ----------
for t in TABLES:
    cs = cols(t)
    typ = dict(cs)
    hs = "md5(concat_ws('|', " + ", ".join(norm_src(c, ty) for c, ty in cs) + "))"
    ht = "md5(concat_ws('|', " + ", ".join(norm_tgt(c, ty) for c, ty in cs) + "))"
    ks = "concat_ws('|', " + ", ".join(norm_src(k, typ[k]) for k in KEYS[t]) + ")"
    kt = "concat_ws('|', " + ", ".join(norm_tgt(k, typ[k]) for k in KEYS[t]) + ")"
    qs = f"select b, md5(string_agg(h, '' order by k, h)) from (select substr(md5({ks}),1,2) b, {ks} k, {hs} h from {t}) group by b"
    qt = f"select b, md5(listagg(h, '') within group (order by k, h)) from (select substr(md5({kt}),1,2) b, {kt} k, {ht} h from {t}) group by b"
    bs, bt = dict(S(qs)), dict(T(qt))
    badb = sorted(b for b in set(bs) | set(bt) if bs.get(b) != bt.get(b))
    fs = md5s = None
    if not badb:
        add("R07", "Row-level hash", t, f"{len(bs)} buckets", "identical", "identical", numeric=False, detail="all rows identical")
        continue
    inb = ", ".join(f"'{b}'" for b in badb)
    rs = collections.Counter(S(f"select {ks} k, {hs} h from {t} where substr(md5({ks}),1,2) in ({inb})"))
    rt = collections.Counter(T(f"select {kt} k, {ht} h from {t} where substr(md5({kt}),1,2) in ({inb})"))
    only_s, only_t = rs - rt, rt - rs
    ks_s, ks_t = {k for k, _ in only_s}, {k for k, _ in only_t}
    missing, extra, changed = ks_s - ks_t, ks_t - ks_s, ks_s & ks_t
    detail = f"{len(missing)} rows only in source, {len(extra)} only in target, {len(changed)} keys with changed values"
    for k in sorted(changed)[:3]:
        a = S(f"select {', '.join(norm_src(c, ty) for c, ty in cs)} from {t} where {ks} = '{k}' limit 1")
        b = T(f"select {', '.join(norm_tgt(c, ty) for c, ty in cs)} from {t} where {kt} = '{k}' limit 1")
        if a and b:
            d = [f"{c}: {x} -> {y}" for (c, _), x, y in zip(cs, a[0], b[0]) if x != y]
            detail += f" | key {k}: " + ", ".join(d[:4])
    add("R07", "Row-level hash", t, f"{len(badb)} of {len(bs)} buckets differ", sum(only_s.values()), sum(only_t.values()), tol=0, detail=detail[:900])
    results[-1] = results[-1][:9] + ("FAIL",) + results[-1][10:]  # any differing row is a failure, whatever the counts
log("R07 done")

# ---------- R08 KPI parity ----------
for name, t, expr, tol in KPIS:
    a, b = S(f"select {expr} from {t}")[0][0], T(f"select {expr} from {t}")[0][0]
    add("R08", "KPI parity", t, name, a, b, tol=tol)
log("R08 done")

# ---------- write results ----------
cur.execute("""create table if not exists berlin_migration.recon.results (
    run_label varchar, check_id varchar, check_name varchar, table_name varchar, metric varchar,
    source_value varchar, target_value varchar, difference float, tolerance float, result varchar,
    detail varchar, checked_at timestamp_ntz default current_timestamp())""")
cur.execute("delete from berlin_migration.recon.results where run_label = %s", (RUN,))
cur.executemany("""insert into berlin_migration.recon.results
    (run_label, check_id, check_name, table_name, metric, source_value, target_value, difference, tolerance, result, detail)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", results)
passed = sum(r[9] == "PASS" for r in results)
print(f"\nRUN {RUN} on schema {SCHEMA}: {len(results)} results, {passed} PASS, {len(results) - passed} FAIL")
by = collections.Counter((r[1], r[9]) for r in results)
for cid in sorted({r[1] for r in results}):
    print(f"  {cid}: PASS {by[(cid, 'PASS')]}  FAIL {by[(cid, 'FAIL')]}")
for r in results:
    if r[9] == "FAIL":
        print(f"  FAIL {r[1]} {r[3]} {r[4]}: {r[5]} vs {r[6]} | {r[10]}")
sf.close()
