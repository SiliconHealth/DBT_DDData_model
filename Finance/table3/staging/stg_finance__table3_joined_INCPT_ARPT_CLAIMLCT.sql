WITH joined_data AS (
    SELECT 
        a.ARNO, 
        a.HN, 
        a.PTTYPE, 
        a.PTTYPEEXT, 
        a.INCDATE, 
        a.SPCFLAG,
        a.RFNDAMT,
        a.WELFAREAMT, 
        b.CLAIMLCT,
        b.NAME as CLAIMLCTNAME,
        DATEPART(YEAR, a.INCDATE) AS year
    FROM {{ source('ddc_internal', 'INCPT') }} a
    JOIN {{ ref('stg_finance__table3_joined_ARPT_CLAIMLCT') }} b ON a.ARNO = b.ARNO
)
SELECT * FROM joined_data
