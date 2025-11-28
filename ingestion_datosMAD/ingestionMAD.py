import psycopg
import pandas as pd

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
    df = pd.read_csv("datos_historicos/MAD/CALIDAD_AIRE_HORARIOS20251013.csv")
    print("Se ha leido correctamente el csv")
except Exception as e:
    print("Error:", e)

try: 
    df = df.drop(columns=["PROVINCIA"])
    print("Se ha eliminado la columna correctamente")
except Exception as e: 
    print("Error:", e)

# Dentro de los datos historicos, los datos llegan hasta el día 31/10/2025.

