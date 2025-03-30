SELECT
IPT.AN
, IPT.HN
, IPT.WEIGHT
, DATEADD(SECOND, CAST(IPT.RGTTIME AS INT) % 100, 
    DATEADD(MINUTE, (CAST(IPT.RGTTIME AS INT) / 100) % 100, 
        DATEADD(HOUR, CAST(IPT.RGTTIME AS INT) / 10000, IPT.RGTDATE)
    )
) AS RGTDATETIME
, LCT.HCODE AS WARDCODE
, LCT.DSPNAME AS WARDNAME
, IPT.ROOM
, IPT.BEDNO
, IPT.DCT
, DATEADD(SECOND, CAST(IPT.INTIME AS INT) % 100, 
    DATEADD(MINUTE, (CAST(IPT.INTIME AS INT) / 100) % 100, 
        DATEADD(HOUR, CAST(IPT.INTIME AS INT) / 10000, IPT.INDATE)
    )
) AS INDATETIME
, DATEADD(SECOND, CAST(IPTSUMMARY.INTIME AS INT) % 100, 
    DATEADD(MINUTE, (CAST(IPTSUMMARY.INTIME AS INT) / 100) % 100, 
        DATEADD(HOUR, CAST(IPTSUMMARY.INTIME AS INT) / 10000, IPTSUMMARY.INDATE)
    )
) AS SUMMARIZED_INDATETIME
, DATEADD(SECOND, CAST(IPT.DCHTIME AS INT) % 100, 
    DATEADD(MINUTE, (CAST(IPT.DCHTIME AS INT) / 100) % 100, 
        DATEADD(HOUR, CAST(IPT.DCHTIME AS INT) / 10000, IPT.DCHDATE)
    )
) AS DCHDATETIME
, DATEADD(SECOND, CAST(IPTSUMMARY.DCHTIME AS INT) % 100, 
    DATEADD(MINUTE, (CAST(IPTSUMMARY.DCHTIME AS INT) / 100) % 100, 
        DATEADD(HOUR, CAST(IPTSUMMARY.DCHTIME AS INT) / 10000, IPTSUMMARY.DCHDATE)
    )
) AS SUMMARIZED_DCHDATETIME
, IPT.DCHTYPE
, DCHTYPE.NAME AS DCHNAME
, IPT.CANCELDATE
, IPT.FIRSTDATE
, IPT.SPCCLINICFLAG
FROM {{ source('ddc_internal', 'IPT') }} IPT
LEFT JOIN {{ source('ddc_internal', 'IPTSUMMARY') }} IPTSUMMARY ON IPTSUMMARY.AN = IPT.AN
LEFT JOIN {{ source('ddc_internal', 'DCHTYPE') }} DCHTYPE ON DCHTYPE.DCHTYPE = IPT.DCHTYPE
LEFT JOIN {{ source('ddc_internal', 'LCT') }} LCT ON LCT.LCT = IPT.WARD