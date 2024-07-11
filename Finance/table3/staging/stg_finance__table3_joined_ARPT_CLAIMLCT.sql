WITH joined_data AS (
    SELECT 
        a.ARNO, 
        a.CLAIMLCT, 
        b.NAME
    FROM {{ source('dbo', 'ARPT') }} a
    JOIN {{ source('dbo', 'CLAIMLCT') }} b ON a.CLAIMLCT = b.CLAIMLCT
)
SELECT * FROM joined_data
