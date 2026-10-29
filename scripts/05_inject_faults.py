"""Phase 4: make a deliberately faulty copy of the legacy data (five typical migration mistakes).
The ORIGINAL legacy.duckdb stays untouched; the faulty copy is what gets loaded as run B."""
import os, shutil, json, duckdb
WORK = os.path.join(os.path.expanduser("~"), "berlin_recon_work")
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC, DST = os.path.join(WORK, "legacy.duckdb"), os.path.join(WORK, "faulty.duckdb")
shutil.copyfile(SRC, DST)
con = duckdb.connect(DST)
pick = lambda t, n: f"rowid in (select rowid from {t} order by hash(rowid) limit {n})"
log = []
def step(num, text, sql):
    con.execute(sql); log.append({"fault": num, "description": text})
step(1, "500 rent-roll rows lost in transit", f"delete from fact_rent_roll where {pick('fact_rent_roll', 500)}")
step(2, "Ticket timestamps shifted by +1 hour (timezone error): created_at, assigned_at, resolved_at",
     "update fact_maintenance_ticket set created_at = created_at + interval 1 hour, assigned_at = assigned_at + interval 1 hour, resolved_at = resolved_at + interval 1 hour")
step(3, "Repair costs rounded to whole euros", "update fact_maintenance_ticket set cost_eur = round(cost_eur)")
step(4, "300 ticket property IDs converted to lowercase (case-sensitivity trap)",
     f"update fact_maintenance_ticket set property_id = lower(property_id) where {pick('fact_maintenance_ticket', 300)}")
step(5, "One batch of 1,000 service-charge statements loaded twice",
     f"insert into fact_service_charge_statement select * from fact_service_charge_statement where {pick('fact_service_charge_statement', 1000)}")
con.close()
with open(os.path.join(HERE, "docs", "injected_faults.json"), "w", encoding="utf-8") as f:
    json.dump(log, f, indent=2)
print("faulty copy written:", DST); [print(" ", x["fault"], x["description"]) for x in log]
