{#
    parse_date_multi_format — versión Databricks / Spark SQL.

    Misma SEMÁNTICA que la versión Postgres: parsea un string a DATE soportando
    los 4 formatos del dataset; lo no parseable -> NULL. Lo que cambia es el
    DIALECTO:

      Postgres                         ->  Spark SQL
      --------------------------------     ----------------------------------
      col ~ '^regex$'                  ->  col rlike '^regex$'
      to_date(col, 'YYYYMMDD')         ->  try_to_date(col, 'yyyyMMdd')
      to_date(col, 'DD/MM/YYYY')       ->  try_to_date(col, 'dd/MM/yyyy')
      to_date(col, 'MM-DD-YYYY')       ->  try_to_date(col, 'MM-dd-yyyy')
      col::date                        ->  try_to_date(col, 'yyyy-MM-dd')

    Dos diferencias importantes:
      1. Los patrones de formato de Spark son SENSIBLES A MAYÚSCULAS:
         yyyy (año), MM (mes), dd (día). 'YYYY'/'DD' son semánticas distintas
         (week-year / day-of-year) y darían resultados silenciosamente malos.
      2. Usamos try_to_date (no to_date): si una fecha pasa el regex pero es
         imposible (ej. mes 13), to_date podría tirar error en modo ANSI;
         try_to_date devuelve NULL, que es justo el contrato "unparseable->NULL".

    Formatos (idénticos al original):
      ISO YYYY-MM-DD   ~91%   inequívoco
      Compact YYYYMMDD ~3%    inequívoco
      Slash DD/MM/YYYY ~3%    LATAM (DMY)
      Dash MM-DD-YYYY  ~3%    US (MDY, confirmado empíricamente)
#}

{% macro parse_date_multi_format(column_name) %}
    case
        when {{ column_name }} is null then null
        when trim({{ column_name }}) = '' then null

        when {{ column_name }} rlike '^\\d{4}-\\d{2}-\\d{2}$'
            then try_to_date({{ column_name }}, 'yyyy-MM-dd')

        when {{ column_name }} rlike '^\\d{8}$'
            then try_to_date({{ column_name }}, 'yyyyMMdd')

        -- Slash = DMY (convención LATAM)
        when {{ column_name }} rlike '^\\d{2}/\\d{2}/\\d{4}$'
            then try_to_date({{ column_name }}, 'dd/MM/yyyy')

        -- Dash = MDY (convención US). Confirmado por filas tipo '11-24-2025'.
        when {{ column_name }} rlike '^\\d{2}-\\d{2}-\\d{4}$'
            then try_to_date({{ column_name }}, 'MM-dd-yyyy')

        else null
    end
{% endmacro %}
