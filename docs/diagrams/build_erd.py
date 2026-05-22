"""
Genera el ERD del proyecto Qversity como PNG.

Salida: docs/diagrams/erd_silver_gold.png  (relativa a la raíz del repo)

Prerequisitos (DOS cosas distintas, ambas obligatorias):

  1. El paquete Python `graphviz`:
         pip install graphviz

  2. El BINARIO Graphviz (el ejecutable `dot`) en el PATH del sistema. El
     paquete pip de arriba es solo un wrapper, no instala el binario.
     - Windows:  winget install graphviz
                 (cerrar y reabrir la terminal después para refrescar PATH)
     - macOS:    brew install graphviz
     - Linux:    sudo apt install graphviz

     Verificación rápida:
         dot -V

Uso (desde la raíz del repo):

    python scripts/build_erd.py

Modelos representados (34 entidades):
  Silver dbt models (16):
    - 3 staging views (stg_*)         -- vistas sobre silver_raw.* de Spark
    - 7 dimensions   (dim_*)
    - 2 facts        (fct_*)
    - 1 aggregate    (agg_*)
    - 3 intermediates (int_*)          -- lógica de negocio compartida

  Gold (18 marts):
    - 18 mart_* tables

Este script es una herramienta de documentación, NO parte del runtime del
pipeline. Si los modelos cambian, editar las definiciones de tablas/edges
abajo y re-correr para regenerar el PNG.
"""
from pathlib import Path
import html
from graphviz import Digraph

g = Digraph(
    "Qversity_ERD",
    graph_attr={
        "rankdir": "TB",
        "fontname": "Helvetica",
        "fontsize": "18",
        "splines": "spline",
        "nodesep": "0.25",
        "ranksep": "0.7",
        "label": ("Qversity ELT - Entity Relationship Diagram\n"
                  "Silver (3 stg + 7 dim + 2 fct + 1 agg + 3 int) + Gold (18 marts)"),
        "labelloc": "t",
        "labeljust": "c",
        "fontcolor": "#1a1a1a",
        "bgcolor": "white",
        "compound": "true",
        "concentrate": "false",
        "ratio": "compress",
        "size": "30,40!",
    },
    node_attr={
        "shape": "plain",
        "fontname": "Helvetica",
        "fontsize": "10",
    },
    edge_attr={
        "fontname": "Helvetica",
        "fontsize": "9",
        "color": "gray40",
    },
)


def html_table(name: str, header_color: str, columns) -> str:
    """
    Build an HTML-like Graphviz label.
    columns: list of (column_name, marker) where marker is "PK", "FK", or "".
    """
    rows = []
    rows.append(
        f'<TR><TD BGCOLOR="{header_color}" ALIGN="CENTER" COLSPAN="2">'
        f'<B>{html.escape(name)}</B></TD></TR>'
    )
    for col, marker in columns:
        col_html = html.escape(col)
        if marker == "PK":
            cell_left = f'<TD ALIGN="LEFT"><B>{col_html}</B></TD>'
            cell_right = '<TD ALIGN="RIGHT"><FONT COLOR="#a04000"><B>PK</B></FONT></TD>'
        elif marker == "FK":
            cell_left = f'<TD ALIGN="LEFT">{col_html}</TD>'
            cell_right = '<TD ALIGN="RIGHT"><FONT COLOR="#1e6091">FK</FONT></TD>'
        else:
            cell_left = f'<TD ALIGN="LEFT" COLSPAN="2">{col_html}</TD>'
            cell_right = ""
        rows.append(f"<TR>{cell_left}{cell_right}</TR>")

    inner = "".join(rows)
    return (
        '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="3">'
        + inner
        + "</TABLE>>"
    )


# Color palette per layer / role
STG_COLOR  = "#d6e4f0"  # light blue: staging views over Spark output
DIM_COLOR  = "#a8c8e8"  # blue: dimensions
FCT_COLOR  = "#f4b07a"  # orange: facts
AGG_COLOR  = "#c8c8c8"  # gray: aggregates
INT_COLOR  = "#cfd8a0"  # olive: intermediates (business logic)
MART_COLOR = "#f4dc6e"  # yellow: gold marts


# ----------------------------------------------------------------------------
# SILVER cluster (16 dbt models)
# ----------------------------------------------------------------------------
with g.subgraph(name="cluster_silver") as silver:
    silver.attr(
        label="Silver (schema: silver — 16 dbt models)",
        style="rounded,filled",
        fillcolor="#f7fafd",
        color="gray45",
        fontcolor="#33567a",
        fontsize="13",
        labeljust="l",
    )

    # ---- Staging views (3) ----
    silver.node("stg_accounts", label=html_table("stg_accounts", STG_COLOR, [
        ("account_id", "PK"),
        ("customer_id", "FK"),
        ("account_type, currency, status", ""),
        ("balance, credit_limit", ""),
        ("interest_rate, opened_date", ""),
        ("branch_code", ""),
    ]))

    silver.node("stg_transactions", label=html_table("stg_transactions", STG_COLOR, [
        ("transaction_id", "PK"),
        ("customer_id", "FK"),
        ("account_id", "FK"),
        ("transaction_date, amount", ""),
        ("currency, transaction_type", ""),
        ("category, merchant", ""),
        ("channel, status", ""),
    ]))

    silver.node("stg_loans", label=html_table("stg_loans", STG_COLOR, [
        ("loan_id", "PK"),
        ("customer_id", "FK"),
        ("loan_type, currency", ""),
        ("principal, outstanding_balance", ""),
        ("interest_rate, term_months", ""),
        ("start_date, end_date", ""),
        ("status, days_past_due", ""),
        ("collateral_type", ""),
    ]))

    # ---- Dimensions (7) ----
    silver.node("dim_customer", label=html_table("dim_customer", DIM_COLOR, [
        ("customer_id", "PK"),
        ("first_name, last_name", ""),
        ("email, phone_number", ""),
        ("date_of_birth, age, age_bucket", ""),
        ("gender, city, country", ""),
        ("is_geo_valid, lat, lon", ""),
        ("registration_date", ""),
        ("tenure_months, tenure_bucket", ""),
        ("kyc_status, risk_score", ""),
        ("customer_segment, status", ""),
    ]))

    silver.node("dim_account", label=html_table("dim_account", DIM_COLOR, [
        ("account_id", "PK"),
        ("customer_id", "FK"),
        ("account_type, currency", ""),
        ("balance, credit_limit", ""),
        ("opened_date, account_age_months", ""),
        ("status", ""),
    ]))

    silver.node("dim_loan", label=html_table("dim_loan", DIM_COLOR, [
        ("loan_id", "PK"),
        ("loan_type, currency", ""),
        ("term_months, collateral_type", ""),
    ]))

    silver.node("dim_credit_info", label=html_table("dim_credit_info", DIM_COLOR, [
        ("customer_id", "PK"),
        ("credit_score", ""),
        ("is_credit_score_valid", ""),
        ("utilization_pct", ""),
        ("is_utilization_pct_valid", ""),
        ("total_limit, total_used", ""),
        ("late_payments_12m, inquiries_6m", ""),
        ("bankruptcy_flag", ""),
    ]))

    silver.node("dim_digital_engagement", label=html_table("dim_digital_engagement", DIM_COLOR, [
        ("customer_id", "PK"),
        ("mobile_app_registered", ""),
        ("web_banking_registered", ""),
        ("last_login_date", ""),
        ("avg_monthly_logins", ""),
        ("preferred_channel", ""),
    ]))

    silver.node("dim_geography", label=html_table("dim_geography", DIM_COLOR, [
        ("country_code", "PK"),
        ("country_name, region", ""),
        ("default_currency", ""),
    ]))

    silver.node("dim_date", label=html_table("dim_date", DIM_COLOR, [
        ("date_day", "PK"),
        ("year, quarter, month", ""),
        ("day_of_week_num, is_weekend", ""),
        ("year_month_key", ""),
    ]))

    # ---- Facts (2) ----
    silver.node("fct_transactions", label=html_table("fct_transactions", FCT_COLOR, [
        ("transaction_id", "PK"),
        ("customer_id", "FK"),
        ("account_id", "FK"),
        ("transaction_date, amount", ""),
        ("currency, transaction_type", ""),
        ("category, merchant", ""),
        ("channel, status", ""),
        ("is_failed, day_of_week", ""),
    ]))

    silver.node("fct_loans", label=html_table("fct_loans", FCT_COLOR, [
        ("loan_id", "PK"),
        ("customer_id", "FK"),
        ("loan_type, currency", ""),
        ("principal, outstanding_balance", ""),
        ("interest_rate", ""),
        ("interest_rate_decimal", ""),
        ("term_months, monthly_payment", ""),
        ("start_date, end_date", ""),
        ("status, days_past_due", ""),
    ]))

    # ---- Aggregate (1) ----
    silver.node("agg_customer_activity", label=html_table("agg_customer_activity", AGG_COLOR, [
        ("customer_id", "PK"),
        ("accounts_count", ""),
        ("transactions_count", ""),
        ("loans_count", ""),
        ("total_products", ""),
    ]))

    # ---- Intermediates (3) ----
    silver.node("int_loan_portfolio_metrics", label=html_table("int_loan_portfolio_metrics", INT_COLOR, [
        ("loan_id", "PK"),
        ("is_delinquent, is_default", ""),
        ("dpd_bucket", ""),
        ("monthly_interest_accrued", ""),
    ]))

    silver.node("int_customer_risk_profile", label=html_table("int_customer_risk_profile", INT_COLOR, [
        ("customer_id", "PK"),
        ("risk_bucket", ""),
        ("credit_score_bucket", ""),
        ("utilization_bucket", ""),
        ("is_delinquent_customer", ""),
        ("is_default_customer", ""),
        ("loans_count", ""),
    ]))

    silver.node("int_customer_monthly_revenue", label=html_table("int_customer_monthly_revenue", INT_COLOR, [
        ("customer_id", "PK"),
        ("monthly_fee_revenue_native", ""),
        ("monthly_fee_revenue_usd", ""),
        ("monthly_interest_revenue_native", ""),
        ("monthly_interest_revenue_usd", ""),
        ("total_monthly_revenue_native", ""),
        ("total_monthly_revenue_usd", ""),
    ]))


# ----------------------------------------------------------------------------
# GOLD cluster (18 marts)
# ----------------------------------------------------------------------------
with g.subgraph(name="cluster_gold") as gold:
    gold.attr(
        label="Gold (schema: gold — 18 marts)",
        style="rounded,filled",
        fillcolor="#fefaf0",
        color="#c08a00",
        fontcolor="#7a5300",
        fontsize="13",
        labeljust="l",
    )

    gold.node("mart_customer_360", label=html_table("mart_customer_360", MART_COLOR, [
        ("customer_id", "PK"),
        ("country, customer_segment", ""),
        ("status, kyc_status", ""),
        ("age_bucket, tenure_bucket", ""),
        ("risk_bucket, credit_score", ""),
        ("credit_score_bucket", ""),
        ("utilization_pct, utilization_bucket", ""),
        ("accounts_count, loans_count", ""),
        ("total_products", ""),
        ("monthly_fee_revenue", ""),
        ("monthly_interest_income", ""),
        ("total_revenue_monthly", ""),
    ]))

    gold.node("mart_acquisition_trend", label=html_table("mart_acquisition_trend", MART_COLOR, [
        ("month", "PK"),
        ("month_label", ""),
        ("new_customers", ""),
        ("cumulative_customers", ""),
        ("mom_growth_pct", ""),
    ]))

    gold.node("mart_revenue_by_segment_usd", label=html_table("mart_revenue_by_segment_usd", MART_COLOR, [
        ("customer_segment", "PK"),
        ("customer_count", ""),
        ("total_revenue_usd", ""),
        ("avg_revenue_per_customer_usd", ""),
        ("total_fee_revenue_usd", ""),
        ("total_interest_revenue_usd", ""),
        ("fee_revenue_share", ""),
    ]))

    gold.node("mart_revenue_monthly_by_segment_usd", label=html_table("mart_revenue_monthly_by_segment_usd", MART_COLOR, [
        ("revenue_month", "PK"),
        ("customer_segment", "PK"),
        ("fee_revenue_usd", ""),
        ("interest_revenue_usd", ""),
        ("total_revenue_usd", ""),
    ]))

    gold.node("mart_account_mix", label=html_table("mart_account_mix", MART_COLOR, [
        ("country", "PK"),
        ("account_type", "PK"),
        ("currency", "PK"),
        ("accounts_count", ""),
        ("accounts_with_balance", ""),
        ("total_balance, avg_balance", ""),
    ]))

    gold.node("mart_delinquency_by_segment", label=html_table("mart_delinquency_by_segment", MART_COLOR, [
        ("customer_segment", "PK"),
        ("customer_count", ""),
        ("delinquent_customers", ""),
        ("delinquency_rate", ""),
    ]))

    gold.node("mart_credit_score_by_country", label=html_table("mart_credit_score_by_country", MART_COLOR, [
        ("country", "PK"),
        ("credit_score_bucket", "PK"),
        ("customer_count", ""),
        ("bucket_share", ""),
    ]))

    gold.node("mart_utilization_vs_delinquency", label=html_table("mart_utilization_vs_delinquency", MART_COLOR, [
        ("utilization_bucket", "PK"),
        ("customer_count", ""),
        ("delinquency_rate", ""),
        ("avg_credit_score", ""),
    ]))

    gold.node("mart_risk_buckets", label=html_table("mart_risk_buckets", MART_COLOR, [
        ("risk_bucket", "PK"),
        ("customer_count", ""),
        ("delinquency_rate", ""),
        ("default_rate", ""),
    ]))

    gold.node("mart_loan_dpd", label=html_table("mart_loan_dpd", MART_COLOR, [
        ("loan_type", "PK"),
        ("dpd_bucket", "PK"),
        ("currency", "PK"),
        ("loan_count", ""),
        ("outstanding_balance", ""),
        ("avg_dpd, bucket_share", ""),
    ]))

    gold.node("mart_loan_composition", label=html_table("mart_loan_composition", MART_COLOR, [
        ("loan_type", "PK"),
        ("status", "PK"),
        ("currency", "PK"),
        ("loan_count", ""),
        ("outstanding_balance", ""),
        ("monthly_interest_total", ""),
    ]))

    gold.node("mart_tx_by_channel", label=html_table("mart_tx_by_channel", MART_COLOR, [
        ("channel", "PK"),
        ("currency", "PK"),
        ("tx_count, total_value", ""),
        ("avg_ticket, failed_rate", ""),
    ]))

    gold.node("mart_tx_by_category", label=html_table("mart_tx_by_category", MART_COLOR, [
        ("category", "PK"),
        ("currency", "PK"),
        ("tx_count, total_value", ""),
        ("avg_ticket", ""),
    ]))

    gold.node("mart_tx_by_dow", label=html_table("mart_tx_by_dow", MART_COLOR, [
        ("day_of_week", "PK"),
        ("currency", "PK"),
        ("tx_count, total_value", ""),
    ]))

    gold.node("mart_international_transfers", label=html_table("mart_international_transfers", MART_COLOR, [
        ("origin_country", "PK"),
        ("tx_currency", "PK"),
        ("tx_count, total_value", ""),
        ("intl_share", ""),
    ]))

    gold.node("mart_digital_adoption_by_segment", label=html_table("mart_digital_adoption_by_segment", MART_COLOR, [
        ("customer_segment", "PK"),
        ("customer_count", ""),
        ("mobile_adoption_rate", ""),
        ("web_adoption_rate", ""),
        ("any_digital_adoption_rate", ""),
    ]))

    gold.node("mart_channel_preference_by_age", label=html_table("mart_channel_preference_by_age", MART_COLOR, [
        ("age_bucket", "PK"),
        ("preferred_channel", "PK"),
        ("customer_count", ""),
        ("bucket_share", ""),
    ]))

    gold.node("mart_top_merchants", label=html_table("mart_top_merchants", MART_COLOR, [
        ("merchant", "PK"),
        ("currency", "PK"),
        ("top_category", ""),
        ("tx_count, total_value", ""),
        ("avg_ticket", ""),
        ("rank_by_value_within_currency", ""),
    ]))


# ----------------------------------------------------------------------------
# Edges
# ----------------------------------------------------------------------------
# Staging promotion to dims/facts (1:1)
g.edge("stg_accounts",     "dim_account",      arrowhead="tee",  arrowtail="tee",  dir="both", color="#5f8cad", style="dashed")
g.edge("stg_transactions", "fct_transactions", arrowhead="tee",  arrowtail="tee",  dir="both", color="#5f8cad", style="dashed")
g.edge("stg_loans",        "fct_loans",        arrowhead="tee",  arrowtail="tee",  dir="both", color="#5f8cad", style="dashed")

# Dim → fact (1:N)
g.edge("dim_customer", "dim_account",      arrowhead="crow", arrowtail="tee", dir="both")
g.edge("dim_customer", "fct_transactions", arrowhead="crow", arrowtail="tee", dir="both")
g.edge("dim_customer", "fct_loans",        arrowhead="crow", arrowtail="tee", dir="both")
g.edge("dim_account",  "fct_transactions", arrowhead="crow", arrowtail="tee", dir="both")
g.edge("dim_loan",     "fct_loans",        arrowhead="tee",  arrowtail="tee", dir="both")

# Customer 1:1 enrichments (dashed to differentiate)
g.edge("dim_customer", "dim_credit_info",        arrowhead="tee", arrowtail="tee", dir="both", color="#5f8cad", style="dashed")
g.edge("dim_customer", "dim_digital_engagement", arrowhead="tee", arrowtail="tee", dir="both", color="#5f8cad", style="dashed")
g.edge("dim_customer", "agg_customer_activity",  arrowhead="tee", arrowtail="tee", dir="both", color="#5f8cad", style="dashed")

# Geography lookup
g.edge("dim_geography", "dim_customer", arrowhead="crow", arrowtail="tee", dir="both")

# Intermediate sources (each int_* enriches one upstream entity)
g.edge("fct_loans",    "int_loan_portfolio_metrics",   arrowhead="tee", arrowtail="tee", dir="both", color="#7a8c4a")
g.edge("dim_customer", "int_customer_risk_profile",    arrowhead="tee", arrowtail="tee", dir="both", color="#7a8c4a")
g.edge("dim_customer", "int_customer_monthly_revenue", arrowhead="tee", arrowtail="tee", dir="both", color="#7a8c4a")


# Gold marts → silver/intermediate (dotted lineage, no constraint)
LIN = "#a89568"
g.edge("mart_customer_360",                "dim_customer",                  style="dotted", color=LIN, constraint="false")
g.edge("mart_customer_360",                "int_customer_monthly_revenue",  style="dotted", color=LIN, constraint="false")
g.edge("mart_acquisition_trend",           "dim_customer",                  style="dotted", color=LIN, constraint="false")
g.edge("mart_revenue_by_segment_usd",      "int_customer_monthly_revenue",  style="dotted", color=LIN, constraint="false")
g.edge("mart_revenue_monthly_by_segment_usd","fct_transactions",            style="dotted", color=LIN, constraint="false")
g.edge("mart_revenue_monthly_by_segment_usd","fct_loans",                   style="dotted", color=LIN, constraint="false")
g.edge("mart_account_mix",                 "dim_account",                   style="dotted", color=LIN, constraint="false")
g.edge("mart_delinquency_by_segment",      "int_customer_risk_profile",     style="dotted", color=LIN, constraint="false")
g.edge("mart_credit_score_by_country",     "int_customer_risk_profile",     style="dotted", color=LIN, constraint="false")
g.edge("mart_utilization_vs_delinquency",  "int_customer_risk_profile",     style="dotted", color=LIN, constraint="false")
g.edge("mart_risk_buckets",                "int_customer_risk_profile",     style="dotted", color=LIN, constraint="false")
g.edge("mart_loan_dpd",                    "int_loan_portfolio_metrics",    style="dotted", color=LIN, constraint="false")
g.edge("mart_loan_composition",            "int_loan_portfolio_metrics",    style="dotted", color=LIN, constraint="false")
g.edge("mart_tx_by_channel",               "fct_transactions",              style="dotted", color=LIN, constraint="false")
g.edge("mart_tx_by_category",              "fct_transactions",              style="dotted", color=LIN, constraint="false")
g.edge("mart_tx_by_dow",                   "fct_transactions",              style="dotted", color=LIN, constraint="false")
g.edge("mart_international_transfers",     "fct_transactions",              style="dotted", color=LIN, constraint="false")
g.edge("mart_digital_adoption_by_segment", "dim_digital_engagement",        style="dotted", color=LIN, constraint="false")
g.edge("mart_channel_preference_by_age",   "dim_digital_engagement",        style="dotted", color=LIN, constraint="false")
g.edge("mart_top_merchants",               "fct_transactions",              style="dotted", color=LIN, constraint="false")


# ----------------------------------------------------------------------------
# Render
# ----------------------------------------------------------------------------
output_dir = Path("docs/diagrams")
output_dir.mkdir(parents=True, exist_ok=True)
output_path = output_dir / "erd_silver_gold"

g.format = "png"
g.render(str(output_path), cleanup=True)
print(f"PNG rendered to {output_path}.png")
