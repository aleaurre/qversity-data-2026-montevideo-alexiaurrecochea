{#
    Bucket a registration_date column into customer tenure segments.

    Buckets:
      - 'new'          < 6 months since registration
      - 'established'  6 to 24 months
      - 'loyal'        > 24 months
      - 'unknown'      when registration_date is NULL

    Computation: month difference between current_date and registration_date.
    `(extract(year from age(...)) * 12 + extract(month from age(...)))` is the
    portable Postgres way to get total months from an interval; the simpler
    `extract(month from ...)` alone returns only the months-component
    (0-11) of the interval, NOT the total months. This is a common bug.

    Tenure as months is also exposed as a separate column (`tenure_months`)
    by the consuming model, so analysts have the raw value alongside the
    bucket.

    Usage:
        select {{ tenure_bucket('registration_date') }} as tenure_bucket
        from ...
#}

{% macro tenure_bucket(date_column) %}
    case
        when {{ date_column }} is null then 'unknown'
        when (extract(year  from age(current_date, {{ date_column }})) * 12
            + extract(month from age(current_date, {{ date_column }}))) < 6  then 'new'
        when (extract(year  from age(current_date, {{ date_column }})) * 12
            + extract(month from age(current_date, {{ date_column }}))) <= 24 then 'established'
        else 'loyal'
    end
{% endmacro %}