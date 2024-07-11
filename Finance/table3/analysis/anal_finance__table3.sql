WITH kcmh AS (
    SELECT 
        year,
        PTTYPEEXT,
        CLAIMLCTNAME,
        TotalAmount,
        UniquePatients
    FROM {{ ref('stg_finance__kcmh') }}
    WHERE PTTYPEEXT IN ('"9000"', '"9001"')
),
non_kcmh AS (
    SELECT 
        year,
        PTTYPEEXT,
        CLAIMLCTNAME,
        TotalAmount,
        UniquePatients
    FROM {{ ref('stg_finance__non_kcmh') }}
    WHERE PTTYPEEXT IN ('"0034"', '"0044"')
),
staff AS (
    SELECT
        year,
        CLAIMLCTNAME,
        SUM(TotalAmount) AS StaffTotalAmount,
        SUM(UniquePatients) AS StaffUniquePatients
    FROM (
        SELECT * FROM kcmh WHERE PTTYPEEXT = '"9000"'
        UNION ALL
        SELECT * FROM non_kcmh WHERE PTTYPEEXT = '"0034"'
    ) t
    GROUP BY year, CLAIMLCTNAME
),
staff_families AS (
    SELECT
        year,
        CLAIMLCTNAME,
        SUM(TotalAmount) AS StaffFamTotalAmount,
        SUM(UniquePatients) AS StaffFamUniquePatients
    FROM (
        SELECT * FROM kcmh WHERE PTTYPEEXT = '"9001"'
        UNION ALL
        SELECT * FROM non_kcmh WHERE PTTYPEEXT = '"0044"'
    ) t
    GROUP BY year, CLAIMLCTNAME
)

SELECT 
    s.year,
    s.CLAIMLCTNAME,
    s.StaffTotalAmount,
    s.StaffUniquePatients,
    n.StaffFamTotalAmount,
    n.StaffFamUniquePatients
FROM staff s
JOIN staff_families n ON s.year = n.year AND s.CLAIMLCTNAME = n.CLAIMLCTNAME
