WITH calidad_aire AS (
    SELECT * FROM {{ ref('int_CalidadAire') }}
)
SELECT * 
FROM calidad_aire
WHERE 
    magnitud IN (1, 6, 7, 8, 9, 10, 12, 14)
    AND (
        (magnitud = 9  AND valor > 10)  -- PM2.5: Superar 10 ya afecta a sensibles
        OR 
        (magnitud = 10 AND valor > 20)  -- PM10: Superar 20 ya afecta a sensibles
        OR 
        (magnitud = 8  AND valor > 40)  -- NO2: Superar 40 empieza a ser nocivo
        OR 
        (magnitud = 14 AND valor > 100) -- O3: Superar 100 afecta a asmáticos
        OR 
        (magnitud = 1  AND valor > 125) -- SO2
        OR
        (magnitud = 6  AND valor > 5)   -- CO (Bajamos el umbral por precaución)
    )