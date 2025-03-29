WITH joined_data AS (
    SELECT 
        a.ARNO, 
        a.CLAIMLCT, 
        b.NAME
    FROM {{ source('ddc_internal', 'ARPT') }} a
    JOIN {{ source('ddc_internal', 'CLAIMLCT') }} b ON a.CLAIMLCT = b.CLAIMLCT
)
SELECT * FROM joined_data
