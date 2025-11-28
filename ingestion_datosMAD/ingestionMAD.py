import psycopg
import pandas as pd
import io 
import os, time 

for i in range(10):
    try:
        #URL CONEXIÓN A BD 
        url = os.getenv("DATABASE_URL")
        #CONEXIÓN A BD
        connection = psycopg.connect(url)
        # Cursor
        cur = connection.cursor()
        print("BD conectada con éxito")
        break
    except Exception as e :
        print("Error conectando a la BD:", e)
        time.sleep(2)

try: 
    # Leemos el CSV
    df = pd.read_csv("CALIDAD_AIRE_HORARIOS20251013.csv", sep=";")
    print("Se ha leido correctamente el csv")
except Exception as e:
    print("Error leyendo CSV:", e)
    sys.exit()

try: 
    # Eliminamos columnas no usadas
    if "PROVINCIA" in df.columns:
        df = df.drop(columns=["PROVINCIA"])
    print("Se ha eliminado la columna correctamente")
except Exception as e: 
    print("Error eliminando columna:", e)

try: 
    # 1. Renombramos las columnas base, PERO dejamos H01...H24 y V01...V24 tal cual
    #    para que la función wide_to_long las encuentre por su prefijo.
    df = df.rename(columns={
        "MUNICIPIO": "municipio_id",
        "ESTACION": "estacion_id",
        "MAGNITUD": "magnitud_id",
        "PUNTO_MUESTREO": "punto_muestreo"
    })
    print("Se han cambiado el nombre de las columnas correctamente")
    
    # --- TRANSFORMACIÓN DE ANCHO A LARGO (UNPIVOT) ---
    
    # 'wide_to_long' busca columnas que empiecen por 'stubnames' (H, V) seguidas de un sufijo numérico.
    # 'i' son las columnas que identifican cada fila única antes de desglosar.
    # 'j' será el nombre de la nueva columna que contendrá el sufijo (la hora, 01, 02...).
    df_long = pd.wide_to_long(
        df, 
        stubnames=['H', 'V'], 
        i=['municipio_id', 'estacion_id', 'magnitud_id', 'punto_muestreo', 'ANO', 'MES', 'DIA'], 
        j='HORA',
        suffix='\d+' # Expresión regular para capturar los números (01, 02, etc)
    )
    
    # wide_to_long deja las claves en el índice, así que reseteamos para tener columnas normales
    df_long = df_long.reset_index()
    
    # Renombramos las columnas resultantes 'H' y 'V' a lo que espera tu base de datos
    df_long = df_long.rename(columns={'H': 'VALOR', 'V': 'VALIDACION'})
    
    # Ordenamos las columnas para asegurar que coinciden con el orden del COPY
    # Nota: Es vital que el orden aquí coincida con el orden en la sentencia SQL COPY
    columnas_finales = [
        'municipio_id', 'estacion_id', 'magnitud_id', 'punto_muestreo', 
        'ANO', 'MES', 'DIA', 'HORA', 'VALOR', 'VALIDACION'
    ]
    df_final = df_long[columnas_finales]
    
    print("Transformación de datos completada exitosamente")

except Exception as e: 
    print("Error en la transformación de datos:", e)
    sys.exit()

# Ingesta en Base de Datos
try: 
    buffer = io.StringIO()
    # Escribimos el dataframe transformado al buffer
    df_final.to_csv(buffer, index=False, header=False, sep=',') # Usamos coma estándar para CSV
    buffer.seek(0)
    
    # Actualizamos la sentencia COPY para el nuevo esquema
    sql = """
    COPY calidad_aire_madrid (
        MUNICIPIO, ESTACION, MAGNITUD, PUNTO_MUESTREO, 
        ANO, MES, DIA, HORA, VALOR, VALIDACION
    )
    FROM STDIN
    WITH (FORMAT CSV, DELIMITER ',')
    """
    
    # Asumimos uso de psycopg 3 por la sintaxis 'with cur.copy...'
    with cur.copy(sql) as copy:
        copy.write(buffer.getvalue())
    
    connection.commit()
    print("Datos históricos ingestados correctamente en formato normalizado")
    
except Exception as e: 
    print("Error en la ingesta SQL:", e)