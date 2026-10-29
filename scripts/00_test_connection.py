import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import snowflake.connector
from config import SNOWFLAKE
con = snowflake.connector.connect(**SNOWFLAKE)
cur = con.cursor()
cur.execute("select current_user(), current_role(), current_warehouse(), current_database(), current_region()")
print(cur.fetchone())
cur.execute("show schemas in database berlin_migration")
print([r[1] for r in cur.fetchall()])
con.close()
