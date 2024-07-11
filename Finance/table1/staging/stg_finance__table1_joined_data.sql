WITH joined_data AS (
    SELECT 
        a.ARNO, 
        a.HN, 
        a.AN, 
        a.PTTYPE, 
        a.VSTDATE, 
        a.SPCFLAG, 
        b.RFNDAMT, 
        b.CUTACTAMT, 
        c.PAIDST,
        DATEPART(MONTH, a.VSTDATE) AS visit_month,
        DATEPART(YEAR, a.VSTDATE) AS visit_year
    FROM {{ source('dbo', 'ARPT') }} a
    JOIN {{ source('dbo', 'ARPTINC') }} b ON a.ARNO = b.ARNO
    JOIN {{ source('dbo', 'INCPT') }} c ON a.ARNO = c.ARNO
)
SELECT * FROM joined_data
