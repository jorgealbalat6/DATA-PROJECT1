WITH calidad_aire AS (
    SELECT * FROM {{ ref('int_CalidadAire') }}
)
SELECT * 
FROM calidad_aire
WHERE 
    magnitud IN (1, 6, 7, 8, 9, 10, 12, 14)
    AND (
        (magnitud = 9  AND valor > 25)  -- PM2.5: Límite alto
        OR 
        (magnitud = 10 AND valor > 50)  -- PM10: Límite diario oficial
        OR 
        (magnitud = 8  AND valor > 200) -- NO2: Umbral de alerta horaria
        OR 
        (magnitud = 14 AND valor > 180) -- O3: Umbral de información pública
        OR 
        (magnitud = 1  AND valor > 350) -- SO2: Umbral de alerta
        OR
        (magnitud = 6  AND valor > 10)  -- CO
    )