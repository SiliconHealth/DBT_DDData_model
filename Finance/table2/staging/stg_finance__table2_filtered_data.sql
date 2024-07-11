WITH filtered_data AS (
    SELECT 
        HN, 
        AN, 
        PTTYPE, 
        INCDATE, 
        SPCFLAG, 
        INCAMT,
        DATEPART(YEAR, INCDATE) AS year,
        DATEPART(MONTH, INCDATE) AS month
    FROM {{ source('dbo', 'INCPT') }}
    WHERE SPCFLAG != 1
)
SELECT * FROM filtered_data

