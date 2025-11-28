import psycopg
import pandas as pd
import io 
import os 

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

try: 
    df = pd.read_csv("CALIDAD_AIRE_HORARIOS20251013.csv", sep=";")
    print("Se ha leido correctamente el csv")
except Exception as e:
    print("Error:", e)

try: 
    df = df.drop(columns=["PROVINCIA"])
    print("Se ha eliminado la columna correctamente")
except Exception as e: 
    print("Error:", e)

try: 
    df = df.rename(columns={
    "MUNICIPIO": "municipio_id",
    "ESTACION": "estacion_id",
    "MAGNITUD": "magnitud_id",
    "PUNTO_MUESTREO": "punto_muestreo"})
    print("Se han cambiado el nombre de las columnas correctamente")
except Exception as e: 
    print("Error:", e)

# Dentro de los datos historicos, los datos llegan hasta el día 31/10/2025.

try: 
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    sql = """
    COPY calidad_aire (municipio_id, estacion_id, magnitud_id, punto_muestreo, ANO, MES, DIA,
    H01, V01, H02, V02, H03, V03, H04, V04, H05, V05, H06, V06, H07, V07, H08, V08, H09, V09, H10, V10,
    H11, V11, H12, V12, H13, V13, H14, V14, H15, V15, H16, V16, H17, V17, H18, V18, H19, V19,
    H20, V20, H21, V21, H22, V22, H23, V23, H24, V24)
    FROM STDIN
    WITH (FORMAT CSV)"""
    with cur.copy(sql) as copy:
        copy.write(buffer.getvalue())
    connection.commit()
    print("Datos históricos ingestados")
except Exception as e: 
    print("Error:", e)