WITH mapa_coordenadas AS (
    -- Mapeo de coordenadas basado en el ID de la estación
    SELECT '4'  AS codigo_estacion, 40.4238823 AS lat_fix, -3.7122567 AS lon_fix UNION ALL -- Pza. de España
    SELECT '8'  , 40.4215533, -3.6823158 UNION ALL -- Escuelas Aguirre
    SELECT '11' , 40.4514734, -3.6773491 UNION ALL -- Av. Ramón y Cajal
    SELECT '16' , 40.4400457, -3.6392422 UNION ALL -- Arturo Soria
    SELECT '17' , 40.3471470, -3.7133167 UNION ALL -- Villaverde Alto
    SELECT '18' , 40.3947825, -3.7318356 UNION ALL -- Farolillo
    SELECT '24' , 40.4193577, -3.7473445 UNION ALL -- Casa de Campo
    SELECT '27' , 40.4769179, -3.5800258 UNION ALL -- Barajas
    SELECT '35' , 40.4192091, -3.7031662 UNION ALL -- Pza. del Carmen
    SELECT '36' , 40.4079517, -3.6453104 UNION ALL -- Moratalaz
    SELECT '38' , 40.4455439, -3.7071303 UNION ALL -- Cuatro Caminos
    SELECT '39' , 40.4782322, -3.7115364 UNION ALL -- Barrio del Pilar
    SELECT '40' , 40.3881478, -3.6515286 UNION ALL -- Vallecas
    SELECT '47' , 40.3980991, -3.6868138 UNION ALL -- Mendez Alvaro
    SELECT '48' , 40.4398904, -3.6903729 UNION ALL -- P Castellana
    SELECT '49' , 40.4144444, -3.6824999 UNION ALL -- Retiro
    SELECT '50' , 40.4655841, -3.6887449 UNION ALL -- Pza. Castilla
    SELECT '54' , 40.3730118, -3.6121394 UNION ALL -- Ensanche de Vallecas
    SELECT '55' , 40.4623628, -3.5805649 UNION ALL -- Urb. Embajada (Barajas)
    SELECT '56' , 40.3850336, -3.7187679 UNION ALL -- Pza. Elíptica
    SELECT '57' , 40.4942012, -3.6605173 UNION ALL -- Sanchinarro
    SELECT '58' , 40.5180701, -3.7746101 UNION ALL -- El Pardo
    SELECT '59' , 40.4651440, -3.6090310 UNION ALL -- Juan Carlos I
    SELECT '60' , 40.5005477, -3.6897308             -- Tres Olivos
),

calidad_aire AS (
    SELECT * FROM {{ ref('stg_CalidadAire') }}
)

SELECT
    ca.id,
    
    -- Lógica de Municipio
    CASE
        WHEN ca.codigo_municipio = 79 THEN 'Madrid'
        WHEN ca.codigo_municipio = 19 THEN 'Barcelona'
        ELSE concat('Municipio ', ca.codigo_municipio)
    END AS municipio,

    ca.MAGNITUD as magnitud,

    -- Lógica de Nombre de Estación
    CASE
        WHEN ca.codigo_estacion = '4' THEN 'Pza. de España'
        WHEN ca.codigo_estacion = '8' THEN 'Escuelas Aguirre'
        WHEN ca.codigo_estacion = '16' THEN 'Arturo Soria'
        WHEN ca.codigo_estacion = '11' THEN 'Av. Ramón y Cajal'
        WHEN ca.codigo_estacion = '17' THEN 'Villaverde Alto'
        WHEN ca.codigo_estacion = '18' THEN 'Farolillo'
        WHEN ca.codigo_estacion = '24' THEN 'Casa de Campo'
        WHEN ca.codigo_estacion = '27' THEN 'Barajas'
        WHEN ca.codigo_estacion = '35' THEN 'Pza. del Carmen'
        WHEN ca.codigo_estacion = '36' THEN 'Moratalaz'
        WHEN ca.codigo_estacion = '38' THEN 'Cuatro Caminos'
        WHEN ca.codigo_estacion = '39' THEN 'Barrio del Pilar'
        WHEN ca.codigo_estacion = '40' THEN 'Vallecas'
        WHEN ca.codigo_estacion = '47' THEN 'Mendez Alvaro'
        WHEN ca.codigo_estacion = '48' THEN 'P Castellana'
        WHEN ca.codigo_estacion = '49' THEN 'Retiro'
        WHEN ca.codigo_estacion = '50' THEN 'Pza. Castilla'
        WHEN ca.codigo_estacion = '54' THEN 'Ensanche de Vallecas'
        WHEN ca.codigo_estacion = '55' THEN 'Urb. Embajada (Barajas)'
        WHEN ca.codigo_estacion = '56' THEN 'Pza. Elíptica'
        WHEN ca.codigo_estacion = '57' THEN 'Sanchinarro'
        WHEN ca.codigo_estacion = '58' THEN 'El Pardo'
        WHEN ca.codigo_estacion = '59' THEN 'Juan Carlos I'
        WHEN ca.codigo_estacion = '60' THEN 'Tres Olivos'
        ELSE concat('Estación ', ca.codigo_estacion)
    END AS estacion,

    -- Lógica de Coordenadas (Prioriza el dato existente, si falta usa el mapa)
    COALESCE(ca.latitud, m.lat_fix) AS latitud,
    COALESCE(ca.longitud, m.lon_fix) AS longitud,

    -- Lógica de Indicador Químico
    CASE
        WHEN ca.MAGNITUD = '1' THEN 'SO2'
        WHEN ca.MAGNITUD = '6' THEN 'CO'
        WHEN ca.MAGNITUD = '7' THEN 'NO'
        WHEN ca.MAGNITUD = '8' THEN 'NO2'
        WHEN ca.MAGNITUD = '9' THEN 'PM2.5'
        WHEN ca.MAGNITUD = '10' THEN 'PM10'
        WHEN ca.MAGNITUD = '12' THEN 'NOx'
        WHEN ca.MAGNITUD = '14' THEN 'O3'
        WHEN ca.MAGNITUD = '20' THEN 'Tolueno'
        WHEN ca.MAGNITUD = '30' THEN 'Benceno'
        WHEN ca.MAGNITUD = '35' THEN 'Etilbenceno'
        WHEN ca.MAGNITUD = '37' THEN 'Metaxileno'
        WHEN ca.MAGNITUD = '38' THEN 'Paraxileno'
        WHEN ca.MAGNITUD = '39' THEN 'Ortoxileno'
        WHEN ca.MAGNITUD = '42' THEN 'Hidrocarburos (Hexano)'
        WHEN ca.MAGNITUD = '43' THEN 'Metano (CH4)'
        WHEN ca.MAGNITUD = '44' THEN 'Hidrocarburos no metánicos'
        WHEN ca.MAGNITUD = '431' THEN 'Metaparaxileno'
        ELSE concat('', ca.MAGNITUD)
    END AS indicador,

    ca.punto_muestreo,

    -- Lógica de Fecha (PostgreSQL sintaxis)
    (make_date(ca.ANO, ca.MES, ca.DIA) + make_interval(hours => ca.HORA - 1)) AS fecha,

    ca.valor,

    CASE
        WHEN ca.datos_disponibles = 'V' THEN true
        ELSE false
    END AS datos_disponibles

FROM calidad_aire ca
LEFT JOIN mapa_coordenadas m
    ON ca.codigo_estacion = m.codigo_estacion