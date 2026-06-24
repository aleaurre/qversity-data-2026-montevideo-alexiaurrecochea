{#
    safe_cast_numeric — versión Databricks / Spark SQL.

    Misma semántica que la versión Postgres (NULL-ea missing markers y strings
    sucios; castea solo lo que matchea un patrón numérico limpio). Cambios de
    dialecto:
      - `~ 'regex'`        -> `rlike 'regex'`
      - `(x)::{{type}}`    -> `cast((x) as {{spark_type}})`
      - Postgres `numeric` no existe en Spark: se mapea a `double`.
        (Si en algún call site pasás 'numeric(18,2)', cambialo a 'decimal(18,2)'.)

    Args:
      column_expr:  expresión de texto (ej. "get_json_object(data, '$.credit_info.x')")
      target_type:  'int' | 'numeric' | 'double' | 'decimal(p,s)'
#}

{% macro safe_cast_numeric(column_expr, target_type) %}
    {%- set spark_type = 'double' if target_type in ('numeric', 'decimal') else target_type -%}
    case
        when ({{ column_expr }}) is null then null
        when ({{ column_expr }}) in ('', 'NA', 'N/A', 'null', 'NULL') then null
        -- Solo castea si matchea un patrón numérico limpio. Cualquier otra cosa
        -- ($, sufijos de moneda, coma decimal) -> NULL, con la DQ trackeada
        -- por las columnas flag aguas abajo.
        when ({{ column_expr }}) rlike '^-?[0-9]+\\.?[0-9]*$' then cast(({{ column_expr }}) as {{ spark_type }})
        else null
    end
{% endmacro %}
