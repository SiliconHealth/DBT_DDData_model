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
    FROM {{ source('ddc_internal', 'INCPT') }}
    WHERE SPCFLAG != 1
)
SELECT * FROM filtered_data

