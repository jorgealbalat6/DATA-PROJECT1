WITH calidad_aire_madrid AS(
SELECT * FROM {{ source('aire_Madrid', 'calidad_aire_madrid')}}
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

FROM calidad_aire_madrid