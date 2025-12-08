WITH calidad_aire AS (
    SELECT * FROM {{ ref('int_CalidadAire') }}
)
SELECT id, municipio, estacion, indicador, fecha, valor, latitud, longitud
FROM calidad_aire
WHERE 
    indicador IN ('PM2.5', 'PM10', 'NO2', 'O3', 'SO2', 'CO')
    AND (
        (indicador = 'PM2.5'  AND valor >= 10)  -- PM2.5: Superar 10 ya afecta a sensibles
        OR 
        (indicador = 'PM10' AND valor >= 20)  -- PM10: Superar 20 ya afecta a sensibles
        OR 
        (indicador = 'NO2'  AND valor >= 40)  -- NO2: Superar 40 empieza a ser nocivo
        OR 
        (indicador = 'O3' AND valor >= 100) -- O3: Superar 100 afecta a asmáticos
        OR 
        (indicador = 'SO2'  AND valor >= 125) -- SO2
        OR
        (indicador = 'CO'  AND valor >= 5)   -- CO (Bajamos el umbral por precaución)
    )