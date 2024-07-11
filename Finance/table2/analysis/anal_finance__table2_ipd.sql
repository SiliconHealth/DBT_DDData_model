WITH grouped_ipd_data AS (
    SELECT 
        year,
        month,
        PTTYPE,
        COUNT(HN) AS visit_count,
        COUNT(DISTINCT HN) AS unique_hn_count,
        SUM(INCAMT) AS total_incam
    FROM {{ ref('stg_finance__table2_filtered_data') }}
    WHERE AN IS NOT NULL AND AN <> 0
    GROUP BY year, month, PTTYPE
)
SELECT * FROM grouped_ipd_data