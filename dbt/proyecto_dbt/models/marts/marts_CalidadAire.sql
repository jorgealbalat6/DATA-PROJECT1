WITH calidad_aire AS (
    SELECT * FROM {{ ref('int_CalidadAire') }}
)
SELECT id, municipio, magnitud, estacion, indicador, fecha, valor, datos_disponibles 
FROM calidad_aire