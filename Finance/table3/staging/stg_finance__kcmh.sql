WITH transactional_table AS (
    SELECT * FROM {{ ref('stg_finance__table3_joined_INCPT_ARPT_CLAIMLCT') }}
)

SELECT
    year,
    PTTYPEEXT,
    CLAIMLCTNAME,
    SUM(RFNDAMT) + SUM(WELFAREAMT) AS TotalAmount,
    COUNT(DISTINCT HN) AS UniquePatients
FROM transactional_table
WHERE PTTYPE IN (1000, 1001)
GROUP BY year, CLAIMLCTNAME, PTTYPEEXT