{% macro revenue_by_country(countries) %}
    {% for country in countries %}
        SUM(CASE WHEN country = '{{ country }}' THEN revenue ELSE 0 END) AS {{ country | lower }}_revenue
    {% endfor %}
{% endmacro %}


SELECT
    id,
    {{ revenue_by_country(['US', 'CA', 'UK']) }}
FROM {{ ref('sales_data') }}
GROUP BY id;
