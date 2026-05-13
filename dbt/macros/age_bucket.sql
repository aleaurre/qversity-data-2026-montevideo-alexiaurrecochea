{#
    Bucket a date_of_birth column into LATAM fintech age segments.

    Buckets:
      - 'under_18'  flag: data quality issue (banks don't onboard minors)
      - '18-25'     students / early career
      - '26-35'     millennials, peak product acquisition
      - '36-50'     peak earning years
      - '51-65'     pre-retirement
      - '65+'       retirees
      - 'unknown'   when date_of_birth is NULL

    Returns a text label. Companion macro `age_bucket_sort_order` (added when
    PowerBI sort is needed) returns an int for ordering.

    Computation:
      `extract(year from age(current_date, date_of_birth))` — Postgres's age()
      returns an interval (years/months/days); extracting years gives completed
      years, which is what "age" colloquially means. This is deterministic
      relative to `current_date` at run time, so the bucket can drift if
      a customer is rebuild-on-the-edge of a bucket. Acceptable for a
      snapshot table refreshed on each DAG run.

    Usage:
        select {{ age_bucket('date_of_birth') }} as age_bucket
        from ...
#}

{% macro age_bucket(date_column) %}
    case
        when {{ date_column }} is null then 'unknown'
        when extract(year from age(current_date, {{ date_column }})) < 18 then 'under_18'
        when extract(year from age(current_date, {{ date_column }})) <= 25 then '18-25'
        when extract(year from age(current_date, {{ date_column }})) <= 35 then '26-35'
        when extract(year from age(current_date, {{ date_column }})) <= 50 then '36-50'
        when extract(year from age(current_date, {{ date_column }})) <= 65 then '51-65'
        else '65+'
    end
{% endmacro %}