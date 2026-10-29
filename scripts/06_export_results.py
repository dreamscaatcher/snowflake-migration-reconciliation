"""Export RECON.RESULTS to CSV files (one per run) so the results can be read without Snowflake."""
import os, sys, csv
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import snowflake.connector
from config import SNOWFLAKE, HERE

con = snowflake.connector.connect(**SNOWFLAKE)
cur = con.cursor()
cur.execute("select distinct run_label from berlin_migration.recon.results order by 1")
for (run,) in cur.fetchall():
    cur.execute("""select check_id, check_name, table_name, metric, source_value, target_value, difference,
                          tolerance, result, detail from berlin_migration.recon.results
                   where run_label = %s order by check_id, table_name, metric""", (run,))
    rows = cur.fetchall()
    path = os.path.join(HERE, "results", f"{run}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["check_id", "check_name", "table_name", "metric", "source_value", "target_value",
                    "difference", "tolerance", "result", "detail"])
        w.writerows(rows)
    print(run, len(rows), "rows ->", path)
cur.execute("select table_name, run_label, rows_source, rows_loaded, errors_seen from berlin_migration.recon.load_log order by run_label, load_id")
with open(os.path.join(HERE, "results", "load_log.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["table_name", "run_label", "rows_source", "rows_loaded", "errors_seen"]); w.writerows(cur.fetchall())
con.close()
