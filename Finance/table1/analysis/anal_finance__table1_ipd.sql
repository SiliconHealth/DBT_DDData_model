WITH base_ipd_data AS (
    SELECT 
        *
    FROM {{ ref('stg_finance__table1_joined_data') }}
    WHERE AN IS NOT NULL AND AN <> 0
),

grouped_ipd_data AS (
    SELECT
        visit_year,
        visit_month,
        COUNT(CASE WHEN PAIDST = 80 THEN 1 ELSE NULL END) AS collected_count,
        SUM(CASE WHEN PAIDST = 80 THEN RFNDAMT ELSE 0 END) AS collected_amount,
        COUNT(CASE WHEN PAIDST = 10 THEN 1 ELSE NULL END) AS unpaid_count,
        SUM(CASE WHEN PAIDST = 10 THEN RFNDAMT ELSE 0 END) AS unpaid_amount,
        COUNT(CASE WHEN PAIDST = 40 THEN 1 ELSE NULL END) AS paid_count,
        SUM(CASE WHEN PAIDST = 40 THEN RFNDAMT ELSE 0 END) AS paid_amount,
        CASE 
            WHEN SUM(CASE WHEN PAIDST = 80 THEN RFNDAMT ELSE 0 END) = 0 
            THEN 0
            ELSE (SUM(CASE WHEN PAIDST = 80 THEN RFNDAMT ELSE 0 END) * 100.0) / SUM(RFNDAMT) 
        END AS collected_percentage
    FROM base_ipd_data
    GROUP BY visit_year, visit_month
)

SELECT 
    visit_year as year,
    visit_month AS month,
    collected_count AS collected_cases,
    collected_amount AS collected_amount,
    unpaid_count AS unpaid_cases,
    unpaid_amount AS unpaid_amount,
    paid_count AS paid_cases,
    paid_amount AS paid_amount,
    ROUND(collected_percentage, 2) AS collected_percentage
FROM grouped_ipd_data;
