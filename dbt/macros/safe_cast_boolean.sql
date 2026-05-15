{#
    safe_cast_boolean
    -----------------
    Casts a text column to boolean, accepting common variants in multiple
    languages and encodings. Returns NULL for unrecognized values rather
    than erroring (Postgres native ::boolean cast errors on anything
    outside true/false/t/f/yes/no/y/n/1/0).

    Discovered when converting dim_digital_engagement from view to table:
    ~3-4% of rows had Spanish ('si'), numeric ('0', '1'), or casing
    variants that broke the native cast. The view was hiding the issue
    because cast was lazy; the table forces evaluation.

    Accepted true values:  true, t, yes, y, sí, si, 1
    Accepted false values: false, f, no, n, 0
    Anything else → NULL (with is_valid flag downstream if needed)

    Usage:
        select {{ safe_cast_boolean("data->>'mobile_app_registered'") }} as mobile_app_registered
        from ...
#}
{% macro safe_cast_boolean(column) %}
    case
        when lower(trim({{ column }})) in ('true', 't', 'yes', 'y', 'sí', 'si', '1') then true
        when lower(trim({{ column }})) in ('false', 'f', 'no', 'n', '0') then false
        else null
    end
{% endmacro %}