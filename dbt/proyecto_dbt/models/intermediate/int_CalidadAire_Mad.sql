
WITH stg_CalidadAire_Mad AS (
    SELECT * FROM {{ ref('stg_CalidadAire_Mad') }}
)

SELECT
    id,
    CASE 
        WHEN codigo_municipio = 79 THEN 'Madrid'
        ELSE concat('Municipio ', codigo_municipio)
    END AS municipio,
    CASE
        WHEN codigo_estacion = '4' THEN 'Pza. de España'
        WHEN codigo_estacion = '8' THEN 'Escuelas Aguirre'
        WHEN codigo_estacion = '16' THEN 'Arturo Soria'
        WHEN codigo_estacion = '11' THEN 'Av. Ramón y Cajal'
        WHEN codigo_estacion = '17' THEN 'Villaverde Alto'
        WHEN codigo_estacion = '18' THEN 'Farolillo'
        WHEN codigo_estacion = '24' THEN 'Casa de Campo'
        WHEN codigo_estacion = '27' THEN 'Barajas'
        WHEN codigo_estacion = '35' THEN 'Pza. del Carmen'
        WHEN codigo_estacion = '36' THEN 'Moratalaz'
        WHEN codigo_estacion = '38' THEN 'Cuatro Caminos'
        WHEN codigo_estacion = '39' THEN 'Barrio del Pilar'
        WHEN codigo_estacion = '40' THEN 'Vallecas'
        WHEN codigo_estacion = '47' THEN 'Mendez Alvaro'
        WHEN codigo_estacion = '48' THEN 'P Castellana'
        WHEN codigo_estacion = '49' THEN 'Retiro'
        WHEN codigo_estacion = '50' THEN 'Pza. Castilla'
        WHEN codigo_estacion = '54' THEN 'Ensanche de Vallecas'
        WHEN codigo_estacion = '55' THEN 'Urb. Embajada (Barajas)'
        WHEN codigo_estacion = '56' THEN 'Pza. Elíptica'
        WHEN codigo_estacion = '57' THEN 'Sanchinarro'
        WHEN codigo_estacion = '58' THEN 'El Pardo'
        WHEN codigo_estacion = '59' THEN 'Juan Carlos I'
        WHEN codigo_estacion = '60' THEN 'Tres Olivos'
        ELSE concat('Estación ', codigo_estacion)
    END AS estacion,
    CASE 
        WHEN MAGNITUD = 1 THEN 'Dióxido de Azufre (SO2)'
        WHEN MAGNITUD = 6 THEN 'Monóxido de Carbono (CO)'
        WHEN MAGNITUD = 7 THEN 'Monóxido de Nitrógeno (NO)'
        WHEN MAGNITUD = 8 THEN 'Dióxido de Nitrógeno (NO2)'
        WHEN MAGNITUD = 9 THEN 'Partículas < 2.5 µm (PM2.5)'
        WHEN MAGNITUD = 10 THEN 'Partículas < 10 µm (PM10)'
        WHEN MAGNITUD = 12 THEN 'Óxidos de Nitrógeno (NOx)'
        WHEN MAGNITUD = 14 THEN 'Ozono (O3)'
        WHEN MAGNITUD = 20 THEN 'Tolueno'
        WHEN MAGNITUD = 30 THEN 'Benceno'
        WHEN MAGNITUD = 35 THEN 'Etilbenceno'
        WHEN MAGNITUD = 37 THEN 'Metaxileno'
        WHEN MAGNITUD = 38 THEN 'Paraxileno'
        WHEN MAGNITUD = 39 THEN 'Ortoxileno'
        WHEN MAGNITUD = 42 THEN 'Hidrocarburos (Hexano)'
        WHEN MAGNITUD = 43 THEN 'Metano (CH4)'
        WHEN MAGNITUD = 44 THEN 'Hidrocarburos no metánicos'
        WHEN MAGNITUD = 431 THEN 'Metaparaxileno'
        ELSE concat('Magnitud ', MAGNITUD)
        END AS indicador_contaminante,
    punto_muestreo,

        -- FECHA UNIFICADA (Año + Mes + Día + Hora)
        -- make_date une la fecha. make_interval suma las horas.
        -- Restamos 1 a la hora porque SQL cuenta 0-23 y la API 1-24.
    
    (make_date(ANO, MES, DIA) + make_interval(hours => HORA - 1)) AS fecha_hora,
    
    valor,

    CASE 
        WHEN datos_disponibles = 'V' THEN true 
        ELSE false 
    END AS datos_disponibles

FROM stg_CalidadAire_Mad