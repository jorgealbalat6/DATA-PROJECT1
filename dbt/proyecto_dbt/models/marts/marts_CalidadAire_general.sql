WITH calidad_aire AS (
    SELECT * FROM {{ ref('int_CalidadAire') }}
)
SELECT id, municipio, estacion, indicador, fecha, valor, latitud, longitud
FROM calidad_aire
WHERE 
    indicador IN ('PM2.5', 'PM10', 'NO2', 'O3', 'SO2', 'CO')
    AND (
        (indicador = 'PM2.5'  AND valor >= 18)  -- PM2.5: Límite alto
        OR 
        (indicador = 'PM10' AND valor >= 35)  -- PM10: Límite diario oficial
        OR 
        (indicador = 'NO2'  AND valor >= 65) -- NO2: Umbral de alerta horaria
        OR 
        (indicador = 'O3' AND valor >= 115) -- O3: Umbral de información pública
        OR 
        (indicador = 'SO2'  AND valor >= 140) -- SO2: Umbral de alerta
        OR
        (indicador = 'CO'  AND valor >= 8)   -- CO
    )