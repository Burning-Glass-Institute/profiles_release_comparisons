# Rerun employer KPI + COMP tables only (with NAICS4 5613 filter)
# Imports shared config from postings_release_temp_tables.py

from postings_release_temp_tables import (
    TOPICS, _V1_FROM, _V2_FROM, _LC_FROM,
    make_kpi_name, make_comp_name, make_comp_lc_name,
    sql_generic_kpis, sql_generic_compare, sql_generic_compare_lc,
    execute_ddl, database, schema,
    user, password, account, warehouse,
)
import snowflake.connector as snow
from datetime import datetime

def main():
    print(f"[{datetime.now()}] Rebuilding employer tables in {database}.{schema}...")

    conn = snow.connect(
        user=user, password=password, account=account,
        warehouse=warehouse, database=database, schema=schema,
    )

    meta = TOPICS["employers"]
    exclude_ilike = meta.get("exclude_values_ilike", [])
    extra_where = meta.get("extra_where")
    total = 0

    for field_name, fmap in meta["fields"].items():
        v1_from = fmap.get("v1_from", _V1_FROM)
        v2_from = fmap.get("v2_from", _V2_FROM)
        lc_from = fmap.get("lc_from", _LC_FROM)

        # KPI table
        kpi_name = make_kpi_name("employers", field_name)
        kpi_sql = sql_generic_kpis(
            fmap["v1_expr"], fmap["v2_expr"], fmap["lc_expr"],
            v1_from, v2_from, lc_from, exclude_ilike, extra_where,
        )
        try:
            execute_ddl(f"CREATE OR REPLACE TABLE {kpi_name} AS\n{kpi_sql}", conn)
            print(f"  ✓ {kpi_name}")
            total += 1
        except Exception as e:
            print(f"  ✗ {kpi_name}: {e}")

        # COMP table
        comp_name = make_comp_name("employers", field_name)
        comp_sql = sql_generic_compare(
            fmap["v1_expr"], fmap["v2_expr"], fmap["lc_expr"],
            v1_from, v2_from, lc_from, exclude_ilike, extra_where,
        )
        try:
            execute_ddl(f"CREATE OR REPLACE TABLE {comp_name} AS\n{comp_sql}", conn)
            print(f"  ✓ {comp_name}")
            total += 1
        except Exception as e:
            print(f"  ✗ {comp_name}: {e}")

        # COMP LC table (top 25 by Lightcast)
        complc_name = make_comp_lc_name("employers", field_name)
        complc_sql = sql_generic_compare_lc(
            fmap["v1_expr"], fmap["v2_expr"], fmap["lc_expr"],
            v1_from, v2_from, lc_from, exclude_ilike, extra_where,
        )
        try:
            execute_ddl(f"CREATE OR REPLACE TABLE {complc_name} AS\n{complc_sql}", conn)
            print(f"  ✓ {complc_name}")
            total += 1
        except Exception as e:
            print(f"  ✗ {complc_name}: {e}")

    conn.close()
    print(f"[{datetime.now()}] Done — created {total} employer tables.")

if __name__ == "__main__":
    main()

