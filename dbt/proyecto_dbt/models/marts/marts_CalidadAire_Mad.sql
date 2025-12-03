WITH calidad_aire AS (
    SELECT * FROM {{ ref('int_CalidadAire_Mad') }}
)
