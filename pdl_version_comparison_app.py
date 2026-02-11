# pdl_version_comparison_app.py
# Streamlit dashboard to compare two PDL releases using pre-computed temp tables
# ── Only change BASE_VERSION and NEW_VERSION below to point at a new comparison ──
#
# ─────────────────────────────────────────────────────────────────────────
import pandas as pd
import altair as alt
import streamlit as st
from snowflake.snowpark import Session

# ─────────────────────────── VERSION CONFIG ───────────────────────────
BASE_VERSION = "v5_OCT25"
NEW_VERSION = "v5_JAN26"

# ── Derived constants (no edits needed below this line) ──────────────
def _alias(version: str) -> str:
    return version.lower().replace("-", "_").replace(" ", "_")

def _label(version: str) -> str:
    return version

def clean_name(s: str) -> str:
    return s.upper().replace(" ", "_").replace("-", "_").replace("'", "")

BASE_A = _alias(BASE_VERSION)
NEW_A = _alias(NEW_VERSION)
DATABASE = "PROJECT_DATA"
SCHEMA = f"PDL_RELEASE_COMPARISONS_{clean_name(BASE_VERSION)}_{clean_name(NEW_VERSION)}"

st.set_page_config(
    page_title=f"PDL {BASE_VERSION} → {NEW_VERSION} Comparison Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────── TABLE NAME HELPERS ─────────────────────────────
def make_comparison_table_name(topic: str, field: str, country: str) -> str:
    return f"COMP_{clean_name(topic)}_{clean_name(field)}_{clean_name(country)}"

def make_kpi_table_name(topic: str, field: str, country: str) -> str:
    return f"KPI_{clean_name(topic)}_{clean_name(field)}_{clean_name(country)}"

def _fq(table: str) -> str:
    """Fully-qualified table path: DATABASE.SCHEMA.TABLE"""
    return f"{DATABASE}.{SCHEMA}.{table}"

# ─────────────────────────── COLOR / AXIS HELPERS ───────────────────────────
_DARK_GREY = "#000000"

_Y_AXIS = dict(
    title=None,
    axis=alt.Axis(
        labelLimit=350,
        labelColor=_DARK_GREY,
        titleColor=_DARK_GREY,
        labelFontSize=13,
        labelFont="Verdana",
    ),
)

def _x_axis(fmt: str | None = None, title: str | None = None) -> alt.Axis:
    axis_kwargs: dict[str, object] = {
        "labelColor": _DARK_GREY,
        "titleColor": _DARK_GREY,
    }
    if fmt is not None:
        axis_kwargs["format"] = fmt
    if title is not None:
        axis_kwargs["title"] = title
    return alt.Axis(**axis_kwargs)

_LEGEND = alt.Legend(labelColor=_DARK_GREY, titleColor=_DARK_GREY)

# ───────────────────────── CUSTOM 5-COLOR PALETTE ────────────────────────────
_RED = "#C22036"
_ORANGE = "#A44914"
_TAN = "#C68C0A"
_BLUE = "#03497A"
_BLACK = "#000000"
FIVE_COLOR_SCALE = [_RED, _ORANGE, _TAN, _BLUE, _BLACK]

COUNTRY_OPTIONS = [
    "United States",
    "United Kingdom",
    "Singapore",
    "Canada",
    "Australia",
    "New Zealand",
    "Switzerland",
    "Hong Kong",
    "China",
]
NULL_LABEL = "<NULL>"

_VERSION_ORDER_DEFAULT = [NEW_VERSION, BASE_VERSION]
_VERSION_ORDER_IPEDS = [NEW_VERSION, BASE_VERSION, "ipeds"]
_VERSION_ORDER_BLS = [NEW_VERSION, BASE_VERSION, "BLS 2023"]
_VERSION_ORDER_ROLES = [NEW_VERSION, BASE_VERSION, "OEWS", "ACS"]

_DEGREE_ORDER = [
    "High School",
    "Certificate",
    "Associate",
    "Bachelor's Degree",
    "Master's Degree",
    "Doctorate",
]

# ───────────────────────── SNOWFLAKE CONNECTION ─────────────────────────
@st.cache_resource(show_spinner=False, ttl="24h")
def get_session() -> Session:
    from snowflake.snowpark.context import get_active_session
    try:
        active = get_active_session()
        if active:
            return active
    except Exception:
        pass

    cfg = dict(
        account="PCA67849",
        user="JULIA_NANIA",
        role="ANALYST",
        warehouse="WH_3_XS",
        database=DATABASE,
        schema=SCHEMA,
        authenticator="externalbrowser",
    )
    return Session.builder.configs(cfg).create()

session = get_session()

# ───────────────────────── TOPIC METADATA ─────────────────────────────
# Table paths built dynamically as pdl_clean.{version}.{table_name}
TOPICS = {
    "dashboard information": {
        "table_name": None,
        "fields": [],
        "country_col": None,
        "id_col": None,
    },
    "roles": {
        "table_name": "experience",
        "fields": [
            "BGI_SOC2_NAME",
            "BGI_ONET_NAME",
            "BGI_SUBOCCUPATION_NAME",
            "BGI_STANDARD_TITLE",
        ],
        "country_col": "BGI_JOB_COUNTRY",
        "id_col": "ID",
    },
    "education": {
        "table_name": "education",
        "fields": [
            "BGI_DEGREE_MAX_PER_ENTRY",
            "BGI_DEGREE_MAX_PER_ID",
            "BGI_DEGREE",
            "BGI_MAJOR_CIP6_NAME",
            "BGI_SCHOOL_NAME",
        ],
        "country_col": "BGI_COUNTRY",
        "id_col": "PERSON_ID",
    },
    "employers": {
        "table_name": "experience",
        "fields": [
            "BGI_NAICS2_NAME",
            "BGI_COMPANY_NAME",
        ],
        "country_col": "BGI_JOB_COUNTRY",
        "id_col": "ID",
    },
    "location": {
        "table_name": "root_person",
        "fields": [
            "BGI_STATE",
            "BGI_COUNTY_NAME",
            "BGI_CITY",
        ],
        "country_col": "BGI_COUNTRY",
        "id_col": "PERSON_ID",
    },
}

# ─────────────────────────── CHART HELPERS ─────────────────────────────
def _prepare(val):
    if isinstance(val, pd.Series):
        return val.fillna(NULL_LABEL).astype(str)
    return NULL_LABEL if pd.isna(val) else str(val)

def _melt(df: pd.DataFrame, value_cols: list[str], value_name: str) -> pd.DataFrame:
    df_disp = df.copy()
    df_disp["field_value"] = _prepare(df_disp["field_value"])
    return df_disp.melt(
        id_vars="field_value",
        value_vars=value_cols,
        var_name="version",
        value_name=value_name,
    )

def _sort_by_new(df: pd.DataFrame, new_col: str) -> list[str]:
    if "cnt_ipeds" in df.columns:
        seen = df["field_value"].fillna(NULL_LABEL).astype(str).tolist()
        order = [d for d in _DEGREE_ORDER if d in seen] + [
            v for v in seen if v not in _DEGREE_ORDER
        ]
        return order
    return (
        df.sort_values(new_col, ascending=False)["field_value"].apply(_prepare).tolist()
    )

def chart_counts(df: pd.DataFrame, title: str):
    value_cols = [f"cnt_{BASE_A}", f"cnt_{NEW_A}"]
    version_order = _VERSION_ORDER_DEFAULT

    if "cnt_ipeds" in df.columns:
        value_cols.append("cnt_ipeds")
        version_order = _VERSION_ORDER_IPEDS

    order = _sort_by_new(df, f"cnt_{NEW_A}")
    long = _melt(df, value_cols, "count")

    version_map = {
        f"cnt_{BASE_A}": _label(BASE_VERSION),
        f"cnt_{NEW_A}": _label(NEW_VERSION),
        "cnt_ipeds": "ipeds",
    }
    long["version"] = long["version"].map(version_map)
    my_colors = FIVE_COLOR_SCALE[: len(version_order)]

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("version:N", sort=version_order),
            x=alt.X("count:Q", axis=_x_axis(title="Count"), stack=None),
            color=alt.Color(
                "version:N",
                title="Version",
                sort=version_order,
                scale=alt.Scale(domain=version_order, range=my_colors),
                legend=_LEGEND,
            ),
            tooltip=["field_value", "version", "count"],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)

def chart_percentages(df: pd.DataFrame, title: str):
    value_cols = [f"cnt_{BASE_A}", f"cnt_{NEW_A}"]
    version_order = _VERSION_ORDER_DEFAULT

    if "cnt_ipeds" in df.columns:
        value_cols.append("cnt_ipeds")
        version_order = _VERSION_ORDER_IPEDS

    totals = {col: df[col].sum() for col in value_cols}
    df_pct = df.copy()
    for col in value_cols:
        suffix = col.replace("cnt_", "")
        df_pct[f"pct_{suffix}"] = df[col] / totals[col] if totals[col] else 0

    order = _sort_by_new(df, f"cnt_{NEW_A}")
    pct_cols = [f"pct_{c.replace('cnt_', '')}" for c in value_cols]
    long = _melt(df_pct, pct_cols, "pct")

    version_map = {
        f"pct_{BASE_A}": _label(BASE_VERSION),
        f"pct_{NEW_A}": _label(NEW_VERSION),
        "pct_ipeds": "ipeds",
    }
    long["version"] = long["version"].map(version_map)
    my_colors = FIVE_COLOR_SCALE[: len(version_order)]

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("version:N", sort=version_order),
            x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Percentage"), stack=None),
            color=alt.Color(
                "version:N",
                title="Version",
                sort=version_order,
                scale=alt.Scale(domain=version_order, range=my_colors),
                legend=_LEGEND,
            ),
            tooltip=["field_value", "version", alt.Tooltip("pct", format=".1%")],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)

def chart_pct_change(df: pd.DataFrame, title: str):
    df_disp = df.copy()
    df_disp["field_value"] = _prepare(df_disp["field_value"])
    order = (
        df_disp.sort_values("pct_change", key=lambda s: s.abs(), ascending=False)[
            "field_value"
        ].tolist()
    )
    chart = (
        alt.Chart(df_disp, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            x=alt.X(
                "pct_change:Q",
                axis=_x_axis(
                    fmt=".1%",
                    title=f"% Change ({NEW_VERSION} − {BASE_VERSION}) / {BASE_VERSION}",
                ),
            ),
            color=alt.condition(
                alt.datum.pct_change > 0,
                alt.value(_BLUE),
                alt.value(_RED),
            ),
            tooltip=["field_value", alt.Tooltip("pct_change", format=".1%")],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)

def chart_role_benchmarks(df: pd.DataFrame, title: str):
    # Dynamic column names from versions
    pct_base_col = f"pct_{BASE_A}"
    pct_new_col = f"pct_{NEW_A}"
    value_cols = [pct_new_col, pct_base_col, "oews_emp_share", "acs_emp_share"]
    version_map = {
        pct_new_col: NEW_VERSION,
        pct_base_col: BASE_VERSION,
        "oews_emp_share": "OEWS",
        "acs_emp_share": "ACS",
    }

    df_disp = df.copy()
    df_disp[value_cols] = df_disp[value_cols] / 100.0
    df_disp["field_value"] = _prepare(df_disp["bgi_soc2_name"])
    order = df_disp.sort_values(pct_new_col, ascending=False)["field_value"]

    long = df_disp.melt(
        id_vars="field_value",
        value_vars=value_cols,
        var_name="version",
        value_name="pct",
    )
    long["version"] = long["version"].map(version_map)

    version_order = _VERSION_ORDER_ROLES
    my_colors = FIVE_COLOR_SCALE[: len(version_order)]

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("version:N", sort=version_order),
            x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Percentage"), stack=None),
            color=alt.Color(
                "version:N",
                title="Version / Benchmark",
                sort=version_order,
                scale=alt.Scale(domain=version_order, range=my_colors),
                legend=_LEGEND,
            ),
            tooltip=["field_value", "version", alt.Tooltip("pct", format=".1%")],
        )
        .properties(height=700)
    )
    st.altair_chart(chart, use_container_width=True)

def chart_employer_industry(df: pd.DataFrame, title: str):
    # Dynamic column names from versions
    perc_base_col = f"perc_{BASE_A}"
    perc_new_col = f"perc_{NEW_A}"
    value_cols = ["perc_bls", perc_base_col, perc_new_col]
    col_to_version = {
        "perc_bls": "BLS 2023",
        perc_base_col: BASE_VERSION,
        perc_new_col: NEW_VERSION,
    }

    order = (
        df.sort_values(perc_new_col, ascending=False)["industry"]
        .apply(_prepare)
        .tolist()
    )

    long = df.melt(
        id_vars="industry", value_vars=value_cols, var_name="source", value_name="pct"
    )
    long["version"] = long["source"].map(col_to_version)

    version_order = _VERSION_ORDER_BLS
    my_colors = FIVE_COLOR_SCALE[: len(version_order)]

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("industry:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("version:N", sort=version_order),
            x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Percentage"), stack=None),
            color=alt.Color(
                "version:N",
                title="Version",
                sort=version_order,
                scale=alt.Scale(domain=version_order, range=my_colors),
                legend=_LEGEND,
            ),
            tooltip=["industry", "version", alt.Tooltip("pct", format=".1%")],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)

# ─────────────────────────── MAIN APP ──────────────────────────────────
def main():
    st.title(f"PDL {BASE_VERSION} vs {NEW_VERSION} Comparison Dashboard")

    topic_options = list(TOPICS.keys())
    topic = st.sidebar.selectbox("Topic:", topic_options, index=0, format_func=str.title)

    if topic == "dashboard information":
        st.header(f"PDL Release Comparison, {BASE_VERSION} → {NEW_VERSION}")
        st.markdown(
            f"""
            This dashboard lets you explore the changes between **{BASE_VERSION}** and **{NEW_VERSION}**.

            It is organised by **topics** which are loosely based on data fields, e.g. *Roles*, *Education*.
            Use the sidebar on the left to select a topic along with a country. You can then choose which data field you would like to look at.
            """
        )
        st.info("Select a topic on the left to get started.")
        return

    country = st.sidebar.selectbox("Country:", COUNTRY_OPTIONS, index=0)
    topic_meta = TOPICS[topic]
    fields = st.sidebar.multiselect(
        "Fields:",
        topic_meta["fields"],
        default=[topic_meta["fields"][0]] if topic_meta["fields"] else [],
    )

    st.caption(
        f"Topic **{topic.title()}**, filtered to **{topic_meta['country_col']} = \"{country}\"**.<br>"
        f"Charts are ordered by {NEW_VERSION} values.",
        unsafe_allow_html=True,
    )

    for field in fields:
        with st.container():
            st.subheader(f"{topic.title()} · Field: {field}")
            if field.upper() == "BGI_STANDARD_TITLE":
                st.info(
                    "Please note that the Standard Title format has changed from **singular** to **plural**, which means there's no direct match between the two versions."
                )

            # Query pre-computed KPI table
            kpi_table = make_kpi_table_name(topic, field, country)
            try:
                kpi_df = session.sql(f"SELECT * FROM {_fq(kpi_table)}").to_pandas()
                cov_base, cov_new, prof_base, prof_new = kpi_df.iloc[0]

                cols = st.columns(4)
                cols[0].metric(f"Coverage {BASE_VERSION}", f"{cov_base:.1%}")
                cols[1].metric(f"Coverage {NEW_VERSION}", f"{cov_new:.1%}")
                cols[2].metric(f"Profiles {BASE_VERSION}", f"{int(prof_base):,}")
                cols[3].metric(f"Profiles {NEW_VERSION}", f"{int(prof_new):,}")
            except Exception as e:
                st.error(f"Error loading KPI data: {e}")
                st.info(f"Expected table: {_fq(kpi_table)}")

            # Query pre-computed comparison table
            comp_table = make_comparison_table_name(topic, field, country)
            try:
                df = session.sql(f"SELECT * FROM {_fq(comp_table)}").to_pandas()
                df.columns = df.columns.str.lower()

                if df.empty:
                    st.info("No data returned.")
                    st.divider()
                    continue

                # Derive helper columns
                df["pct_change"] = (
                    (df[f"cnt_{NEW_A}"] - df[f"cnt_{BASE_A}"]) / df[f"cnt_{BASE_A}"]
                ).where(df[f"cnt_{BASE_A}"] != 0)

                df_top25 = df.sort_values(f"cnt_{NEW_A}", ascending=False).head(25)

                chart_percentages(
                    df_top25,
                    f"Top 25 Values – Percentages ({NEW_VERSION} vs {BASE_VERSION})",
                )
                chart_counts(
                    df_top25,
                    f"Top 25 Values – Counts ({NEW_VERSION} vs {BASE_VERSION})",
                )

                df_change = (
                    df.dropna(subset=["pct_change"])
                    .assign(abs_change=lambda d: d["pct_change"].abs())
                    .sort_values("abs_change", ascending=False)
                    .head(20)
                )
                if not df_change.empty:
                    chart_pct_change(
                        df_change,
                        f"Largest Percentage Differences ({NEW_VERSION} vs {BASE_VERSION})",
                    )

            except Exception as e:
                st.error(f"Error loading comparison data: {e}")
                st.info(f"Expected table: {_fq(comp_table)}")

            st.divider()

    # Extra roles-specific chart (SOC-2 vs OEWS/ACS)
    if topic == "roles":
        with st.container():
            st.subheader("Roles · SOC-2 Distribution vs OEWS & ACS Benchmarks")
            try:
                roles_df = session.sql(f"SELECT * FROM {_fq('ROLES_BENCHMARK_OEWS_ACS')}").to_pandas()
                roles_df.columns = roles_df.columns.str.lower()

                if roles_df.empty:
                    st.info("No data returned for Roles benchmark chart.")
                else:
                    pct_new_col = f"pct_{NEW_A}"
                    roles_df = roles_df.sort_values(pct_new_col, ascending=False).head(25)
                    chart_role_benchmarks(
                        roles_df,
                        f"SOC-2 Employment Share: {NEW_VERSION} vs {BASE_VERSION} vs OEWS vs ACS",
                    )
            except Exception as e:
                st.error(f"Error loading roles benchmark data: {e}")
                st.info(f"Expected table: {_fq('ROLES_BENCHMARK_OEWS_ACS')}")

            st.divider()

    # Extra employer-specific chart (Industry vs BLS)
    if topic == "employers":
        with st.container():
            st.subheader("Employer · Industry Distribution")
            try:
                bls_df = session.sql(f"SELECT * FROM {_fq('EMPLOYERS_INDUSTRY_BLS')}").to_pandas()
                bls_df.columns = bls_df.columns.str.lower()

                if bls_df.empty:
                    st.info("No data returned for Employer Industry chart.")
                else:
                    perc_new_col = f"perc_{NEW_A}"
                    bls_df = bls_df.sort_values(perc_new_col, ascending=False).head(20)
                    chart_employer_industry(bls_df, "Comparison with BLS, Year = 2023")
            except Exception as e:
                st.error(f"Error loading employer industry data: {e}")
                st.info(f"Expected table: {_fq('EMPLOYERS_INDUSTRY_BLS')}")

            st.divider()

# ───────────────────────── RUN APP ─────────────────────────────────────
if __name__ == "__main__":
    main()
