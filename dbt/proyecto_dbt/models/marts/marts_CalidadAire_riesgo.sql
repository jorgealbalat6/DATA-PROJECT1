WITH calidad_aire AS (
    SELECT * FROM {{ ref('int_CalidadAire') }}
)
SELECT id, municipio, magnitud, estacion, indicador, fecha, valor, datos_disponibles, latitud, longitud
FROM calidad_aire
WHERE 
    magnitud IN ('1', '6', '7', '8', '9', '10', '12', '14')
    AND (
        (magnitud = 'PM2.5'  AND valor > 10)  -- PM2.5: Superar 10 ya afecta a sensibles
        OR 
        (magnitud = 'PM10' AND valor > 20)  -- PM10: Superar 20 ya afecta a sensibles
        OR 
        (magnitud = 'NO2'  AND valor > 40)  -- NO2: Superar 40 empieza a ser nocivo
        OR 
        (magnitud = 'O3' AND valor > 100) -- O3: Superar 100 afecta a asmáticos
        OR 
        (magnitud = 'SO2'  AND valor > 125) -- SO2
        OR
        (magnitud = 'CO'  AND valor > 5)   -- CO (Bajamos el umbral por precaución)
    )