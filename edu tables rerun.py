import snowflake.connector as snow
from postings_release_temp_tables import (
    execute_ddl, sql_generic_kpis, make_kpi_name,
    TOPICS, _V1_FROM, _V2_FROM, _LC_FROM,
    user, password, account, warehouse, database, schema
)

# Connect
conn = snow.connect(
    user=user, password=password, account=account,
    warehouse=warehouse, database=database, schema=schema
)

# Pull education topic config
meta = TOPICS["education"]
fmap = meta["fields"]["MIN_REQUIRED_DEGREE"]
exclude_ilike = meta.get("exclude_values_ilike", [])

# Build and run the KPI query
kpi_name = make_kpi_name("education", "MIN_REQUIRED_DEGREE")
kpi_sql = sql_generic_kpis(
    fmap["v1_expr"], fmap["v2_expr"], fmap["lc_expr"],
    fmap.get("v1_from", _V1_FROM),
    fmap.get("v2_from", _V2_FROM),
    fmap.get("lc_from", _LC_FROM),
    exclude_ilike
)
execute_ddl(f"CREATE OR REPLACE TABLE {kpi_name} AS\n{kpi_sql}", conn)
print(f"Done: {kpi_name}")

conn.close()