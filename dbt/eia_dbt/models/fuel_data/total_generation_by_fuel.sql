-- eia_dbt/models/fuel_data/total_generation_by_fuel.sql

SELECT
    fuel_type,
    DATE_TRUNC('month', period) AS month,
    SUM(value) AS total_generation_mwh
FROM {{ ref('raw_eia_fuel_type_data') }}
GROUP BY fuel_type, month
ORDER BY month DESC, fuel_type
