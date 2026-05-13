{#
    Parse a string column into a Postgres DATE, supporting the FOUR formats
    found in the source dataset:

      Format               Locale  Example      ~Share  Notes
      -------------------  ------  -----------  ------  --------------------------------
      ISO YYYY-MM-DD       n/a     2026-05-08   ~91%    unambiguous
      Compact YYYYMMDD     n/a     20260613     ~3%     unambiguous
      Slash DD/MM/YYYY     DMY     26/04/2026   ~3%     LATAM convention
      Dash  MM-DD-YYYY     MDY     06-13-2024   ~3%     US convention (confirmed by
                                                        rows with first comp > 12)

    Unknown / unparseable values return NULL.

    Why this lives in dbt and not Spark:
    Choosing which formats are "valid" and which become NULL is a SEMANTIC
    decision, not a syntactic one. Per the project's responsibility split
    documented in decisions.md, semantic decisions live in dbt. Spark
    delivers raw strings to silver_raw; dbt parses them into proper types
    on the way to the silver dimensional/fact layer.

    Used by:
      - dim_customer.sql       (date_of_birth, registration_date)
      - stg_accounts.sql       (opened_date)
      - stg_transactions.sql   (transaction_date)
      - stg_loans.sql          (start_date, end_date)

    A custom test should be added per-column to monitor the NULL rate
    (when Spark's strings don't match any format, we lose the row). If the
    NULL rate climbs significantly between runs, the regex set needs
    reviewing — likely a new format introduced by the source.

    Usage:
        select {{ parse_date_multi_format('opened_date') }} as opened_date
        from ...
#}

{% macro parse_date_multi_format(column_name) %}
    case
        when {{ column_name }} is null then null
        when {{ column_name }} = '' then null

        when {{ column_name }} ~ '^\d{4}-\d{2}-\d{2}$'
            then ({{ column_name }})::date

        when {{ column_name }} ~ '^\d{8}$'
            then to_date({{ column_name }}, 'YYYYMMDD')

        -- Slash = DMY (LATAM convention)
        when {{ column_name }} ~ '^\d{2}/\d{2}/\d{4}$'
            then to_date({{ column_name }}, 'DD/MM/YYYY')

        -- Dash = MDY (US convention). Confirmed by rows like '11-24-2025'
        -- (month=11 day=24) which disprove the DMY hypothesis for dash.
        when {{ column_name }} ~ '^\d{2}-\d{2}-\d{4}$'
            then to_date({{ column_name }}, 'MM-DD-YYYY')

        else null
    end
{% endmacro %}