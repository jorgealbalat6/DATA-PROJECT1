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
    df = pd.read_csv("CALIDAD_AIRE_HORARIOS20251013.csv", sep=";")
    print("Se ha leido correctamente el csv")
except Exception as e:
    print("Error leyendo CSV:", e)

try: 
    if "PROVINCIA" in df.columns:
        df = df.drop(columns=["PROVINCIA"])
    print("Se ha eliminado la columna correctamente")
except Exception as e: 
    print("Error eliminando columna:", e)

try: 
    print("Transformando datos con Pandas...")
    df_long = pd.wide_to_long(
        df, 
        stubnames=['H', 'V'], 
        i=['MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 'ANO', 'MES', 'DIA'], 
        j='HORA',
        suffix=r'\d+'
    )

    df_long = df_long.reset_index()

    df_long = df_long.rename(columns={'H': 'VALOR', 'V': 'VALIDACION'})

    columnas_finales = [
        'MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 
        'ANO', 'MES', 'DIA', 'HORA', 'VALOR', 'VALIDACION'
    ]

    df_final = df_long[columnas_finales].dropna(subset=['VALOR'])
    
    print(f"Transformación completada. Filas resultantes: {len(df_final)}")

except Exception as e: 
    print("Error en la transformación de datos:", e)
    exit()

try: 
    buffer = io.StringIO()

    df_final.to_csv(buffer, index=False, header=False, sep=',')
    buffer.seek(0)
    
    sql = """
    COPY calidad_aire_madrid (
        MUNICIPIO, ESTACION, MAGNITUD, PUNTO_MUESTREO, 
        ANO, MES, DIA, HORA, VALOR, VALIDACION
    )
    FROM STDIN
    WITH (FORMAT CSV, DELIMITER ',')
    """
    
    with cur.copy(sql) as copy:
        copy.write(buffer.getvalue())
    
    connection.commit()
    print("Datos históricos ingestados correctamente en formato normalizado")
    
except Exception as e: 
    print("Error en la ingesta SQL:", e)