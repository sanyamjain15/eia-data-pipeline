-- eia_dbt/models/sales_data/monthly_sales_summary.sql

SELECT
    period,
    state_desc,
    sector_name,
    SUM(customers) AS total_customers,
    SUM(revenue) AS total_revenue,
    SUM(sales) AS total_sales_mwh,
    ROUND(SUM(revenue) / NULLIF(SUM(sales), 0), 4) AS avg_price_per_mwh
FROM {{ ref('raw_eia_monthly_sales') }}
GROUP BY period, state_desc, sector_name
ORDER BY period DESC, state_desc, sector_name
