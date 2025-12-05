WITH calidad_aire AS(
SELECT * FROM {{ source('aire', 'calidad_aire')}}
)

SELECT
    id,
    MUNICIPIO AS codigo_municipio,
    ESTACION AS codigo_estacion,
    MAGNITUD,
    PUNTO_MUESTREO AS punto_muestreo,
    ANO,
    MES,
    DIA,
    HORA,
    VALOR AS valor,
    VALIDACION AS datos_disponibles
FROM calidad_aire