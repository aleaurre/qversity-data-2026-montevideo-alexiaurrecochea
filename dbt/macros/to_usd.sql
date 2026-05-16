{# 
  Converts a monetary amount from its native currency to USD using the 
  fx_rates seed. Use as a SELECT-list expression in models.

  Usage:
    {{ to_usd('t.amount', 't.currency') }} AS amount_usd

  Implementation note: this returns a correlated subquery for readability
  in a single-column context. For models that need many currency 
  conversions or aggregate over millions of rows, prefer an explicit
  LEFT JOIN to {{ ref('fx_rates') }} once and reuse the rate.
#}
{% macro to_usd(amount_col, currency_col) %}
  (
    {{ amount_col }} * (
      SELECT rate_to_usd 
      FROM {{ ref('fx_rates') }} 
      WHERE currency = {{ currency_col }}
    )
  )
{% endmacro %}