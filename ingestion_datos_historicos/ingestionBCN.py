import psycopg
import pandas as pd
import io 
import os, time 

for i in range(10):
    try:
        url = os.getenv("DATABASE_URL")
        connection = psycopg.connect(url)
        cur = connection.cursor()
        print("BD conectada con éxito")
        break
    except Exception as e :
        print("Error conectando a la BD:", e)
        time.sleep(2)

try:
    df_historico_BCN = pd.read_csv("Qualitat_de_l’aire_als_punts_de_mesurament_manuals_de_la_Xarxa_de_Vigilància_i_Previsió_de_la_Contaminació_Atmosfèrica_20251206.csv", sep=",")
    print("Se han leido correctamente el csv de Barcelona")
except Exception as e:
    print("Error leyendo el csv de Barcelona:", e)

try:
    df_historico_BCN = df_historico_BCN[df_historico_BCN['NOM ESTACIÓ'].str.startswith('Barcelona', na=False)]
    print("Se han eliminado correctamente los datos que no pertenecen a Barcelona")
except Exception as e:
    print("Error al eliminar los datos que no son de Barcelona:", e)


