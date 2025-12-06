WITH calidad_aire AS (
    SELECT * FROM {{ ref('int_CalidadAire') }}
)
SELECT id, municipio, estacion, indicador, fecha, valor, latitud, longitud
FROM calidad_aire