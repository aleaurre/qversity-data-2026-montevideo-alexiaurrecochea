{#
    Normalize transaction.transaction_type to its 6 canonical lowercase
    English values: 'deposit', 'withdrawal', 'transfer', 'payment',
    'refund', 'fee'.

    The bronze dataset contains 28 surface variants across those 6 canonical
    values — the most chaotic categorical in the dataset:
      - Casing chaos: 'DEPOSIT', 'Deposit', etc. (collapsed by normalize_casing)
      - Spanish translations (mapped here):
          deposito       -> deposit
          retiro         -> withdrawal
          transferencia  -> transfer
          pago           -> payment
          reembolso      -> refund
          comision       -> fee

    Note: 'pago' is also "pago" in lowercase form — handled by normalize_casing
    before reaching the CASE mapping.

    Usage:
        select {{ normalize_transaction_type('transaction_type') }} as transaction_type
        from ...
#}

{% macro normalize_transaction_type(column_name) %}
    case {{ normalize_casing(column_name) }}
        when 'deposito'      then 'deposit'
        when 'retiro'        then 'withdrawal'
        when 'transferencia' then 'transfer'
        when 'pago'          then 'payment'
        when 'reembolso'     then 'refund'
        when 'comision'      then 'fee'
        else {{ normalize_casing(column_name) }}
    end
{% endmacro %}