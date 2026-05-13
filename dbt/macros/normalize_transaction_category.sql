{#
    Normalize transaction.category by NULL-ing the four "missing" indicators
    used by the data generator, and lowercasing the rest.

    The bronze dataset uses inconsistent missing-value markers:
      - empty string ''
      - the literal string 'NA'
      - the literal string 'null'
      - the literal string 'N/A'

    Combined these represent ~3,300 rows (~3.7% of transactions). Treating
    them as legitimate categories would pollute downstream aggregations
    (e.g. "transactions by category" would show 4 phantom categories).

    Strategy:
      1. normalize_casing collapses casing.
      2. CASE-statement converts the 4 missing markers to SQL NULL.
      3. Real categories pass through. The 15 canonical categories from
         the spec ('healthcare', 'dining', 'utilities', etc.) are not
         enumerated here — the schema.yml test will enforce the closed
         list if/when added.

    Why not normalize at the spark layer:
    Spark's syntactic cleaning trims whitespace but doesn't reclassify
    string values. Reclassifying 'N/A' as NULL is a semantic decision
    (we're saying "we treat these as unknown"), which belongs in dbt.

    Usage:
        select {{ normalize_transaction_category('category') }} as category
        from ...
#}

{% macro normalize_transaction_category(column_name) %}
    case {{ normalize_casing(column_name) }}
        when ''     then null
        when 'na'   then null
        when 'null' then null
        when 'n/a'  then null
        else {{ normalize_casing(column_name) }}
    end
{% endmacro %}