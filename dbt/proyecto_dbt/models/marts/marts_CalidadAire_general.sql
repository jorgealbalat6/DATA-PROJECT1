WITH calidad_aire AS (
    SELECT * FROM {{ ref('int_CalidadAire') }}
)
SELECT id, municipio, magnitud, estacion, indicador, fecha, valor, datos_disponibles, latitud, longitud
FROM calidad_aire
WHERE 
    magnitud IN ('1', '6', '7', '8', '9', '10', '12', '14')
    AND (
        (magnitud = '9'  AND valor >= 18)  -- PM2.5: Límite alto
        OR 
        (magnitud = '10' AND valor >= 35)  -- PM10: Límite diario oficial
        OR 
        (magnitud = '8'  AND valor >= 65) -- NO2: Umbral de alerta horaria
        OR 
        (magnitud = '14' AND valor >= 115) -- O3: Umbral de información pública
        OR 
        (magnitud = '1'  AND valor >= 140) -- SO2: Umbral de alerta
        OR
        (magnitud = '6'  AND valor >= 8)   -- CO
    )