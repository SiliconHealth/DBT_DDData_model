WITH IPTSUMMARY_DATE AS (
    SELECT IPTSUMMARY.AN
        ,  IPTSUMMARY.INDATE
        ,  IPTSUMMARY.DCHDATE
    FROM {{ source('ddc_internal', 'IPTSUMMARY') }} IPTSUMMARY
)
SELECT 
IPTSUMDIAG.AN
, IPTSUMMARY_DATE.INDATE
, IPTSUMMARY_DATE.DCHDATE
, IPTSUMDIAG.DIAGTYPE
, DIAGTYPE.NAME AS DIAGTYPE_NAME
, IPTSUMDIAG.ITEMNO
, IPTSUMDIAG.ICD10
, ICD10.ICD10WHO
, ICD10.NAME AS DISEASE_NAME
, IPTSUMDIAG.DIAGKEY
, IPTSUMDIAG.FIRSTDATE
FROM {{ source('ddc_internal', 'IPTSUMDIAG') }} IPTSUMDIAG
LEFT JOIN IPTSUMMARY_DATE ON IPTSUMMARY_DATE.AN = IPTSUMDIAG.AN
LEFT JOIN {{ source('ddc_internal', 'DIAGTYPE') }} DIAGTYPE ON DIAGTYPE.DIAGTYPE = IPTSUMDIAG.DIAGTYPE
LEFT JOIN {{ source('ddc_internal', 'ICD10') }} ICD10 ON ICD10.ICD10 = IPTSUMDIAG.ICD10

