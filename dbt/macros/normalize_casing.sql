{#
    Base normalization macro: lowercase + trim.

    This is the structural cleaning step shared by all categorical
    normalization macros in this project (status, type, segment fields
    across customer, account, transaction, loan, kyc).

    Why it exists as a standalone macro:
    Every categorical in this dataset arrives with casing chaos from
    the generator (`Active`, `ACTIVE`, etc.). Many also have Spanish
    translations layered on top (`activo`, `suspendido`). Separating
    *structural* cleaning (this macro) from *semantic* mapping (per-entity
    case statements) keeps each layer focused:

      - normalize_casing: handles the 'how it was typed' problem.
      - normalize_<entity>_<field>: handles the 'how it was named' problem
        (Spanish vs English, synonyms, etc.), built on top of this base.

    For entities whose categorical only has casing variants and no
    language mapping (e.g. kyc_status, transaction.status, loan.status,
    loan.type), the specific macro is a thin wrapper that just calls this.
    That's intentional: a thin wrapper per entity makes intent explicit
    at the call site (`normalize_kyc_status(col)` is more discoverable
    than `normalize_casing(col)` in a model).

    Usage (rarely called directly; usually wrapped):
        select {{ normalize_casing('some_column') }} as some_column
        from ...
#}

{% macro normalize_casing(column_name) %}
    lower(trim({{ column_name }}))
{% endmacro %}