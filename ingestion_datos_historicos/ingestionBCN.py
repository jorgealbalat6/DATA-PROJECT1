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
    df_historico_BCN.columns = df_historico_BCN.columns.str.strip()
    print("Columnas detectadas:", df_historico_BCN.columns.tolist())
except Exception as e:
    print("Error leyendo el csv de Barcelona:", e)

try:
    df_historico_BCN = df_historico_BCN[df_historico_BCN['NOM MUNICIPI']=='Barcelona']
    print("Se han eliminado correctamente los datos que no pertenecen a Barcelona")
except Exception as e:
    print("Error al eliminar los datos que no son de Barcelona:", e)

try:
    if "UNITATS" in df_historico_BCN:
        df_historico_BCN = df_historico_BCN.drop(columns=["UNITATS"])
    print("Se ha eliminado la columna UNITATS correctamente")
except Exception as e:
    print("Error eliminando la columna UNITATS:", e)

try: 
    if "TIPUS ESTACIÓ" in df_historico_BCN: 
        df_historico_BCN = df_historico_BCN.drop(columns=["TIPUS ESTACIÓ"])
    print("Se ha eliminado la columna TIPUS ESTACIÓ correctamente")
except Exception as e:
    print("Error eliminando la columna TIPUS ESTACIÓ:", e)

try: 
    if "CODI INE" in df_historico_BCN:
        df_historico_BCN = df_historico_BCN.drop(columns=["CODI INE"])
    print("Se ha eliminado la columna CODI INE correctamente")
except Exception as e:
    print("Error eliminando la columna CODI INE:", e)

try: 
    if "Georeferència" in df_historico_BCN:
        df_historico_BCN = df_historico_BCN.drop(columns=["Georeferència"])
    print("Se ha eliminado la columna Georeferència correctamente")
except Exception as e: 
    print("Error eliminando la columna Georeferència:", e)

try: 
    if "NOM CONTAMINANT" in df_historico_BCN:
        df_historico_BCN = df_historico_BCN.drop(columns=["NOM CONTAMINANT"])
    print("Se ha eliminado la columna NOM CONTAMINANT correctamente")
except Exception as e:
    print("Error eliminando la columna NOM CONTAMINANT:", e)

try: 
    lista = ["1", "6", "7", "8", "9", "10", "12", "14", "20", "30", "35", "37", "38", "39", "42", "43", "44", "431"]
    df_historico_BCN = df_historico_BCN[df_historico_BCN['MAGNITUD'].isin(lista)]
    print("Eliminados los contaminantes sin importancia")
except Exception as e:
    print("Error eliminando los contaminantes sin importancia:", e)

try:
    df_historico_BCN = df_historico_BCN.rename(columns={
        "CODI EOI": "PUNTO_MUESTREO",
        "NOM ESTACIÓ": "ESTACION",
        "ANY": "ANO", 
        "NOM MUNICIPI": "MUNICIPIO",
        "LATITUD": "LAT",
        "LONGITUD": "LON"
    })
    print("Nombre de las variables cambiado")
except Exception as e:
    print("Error cambiando nombre de variables:", e)

try:
    df_long = pd.wide_to_long(
        df_historico_BCN, 
        stubnames=["D"],
        i=['MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 'ANO', 'MES', 'LAT', 'LON'],
        j="DIA",
        suffix=r'\d+'
    )
    df_long = df_long.rename(columns={'D': 'VALOR'})
    df_long = df_long.reset_index()
    columnas_finales = [
        'MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 
        'ANO', 'MES', 'DIA', 'VALOR', 'LAT', 'LON'
    ]
    df_final = df_long[columnas_finales].dropna(subset=['VALOR'])
    df_final['HORA'] = 0
    df_final['VALIDACION'] = 'V'
    columnas_finales = [
        'MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO',
        'ANO', 'MES', 'DIA', 'HORA', 'VALOR', 'VALIDACION', 'LAT', 'LON'
    ]
    df_final = df_final[columnas_finales]
    print(f"Transformación completada. Filas resultantes: {len(df_final)}")
except Exception as e: 
    print("Error en la transformación de datos:", e)
    exit() 


try:
    buffer = io.StringIO()
    df_final.to_csv(buffer, index=False, header=False, sep=',')
    buffer.seek(0)

    sql = """
    COPY calidad_aire (
        MUNICIPIO, ESTACION, MAGNITUD, PUNTO_MUESTREO, 
        ANO, MES, DIA, HORA, VALOR, VALIDACION, LAT, LON
    )
    FROM STDIN
    WITH (FORMAT CSV, DELIMITER ',')
    """

    with cur.copy(sql) as copy:
        copy.write(buffer.getvalue())

    connection.commit()
    print("Datos históricos de Barcelona ingestados correctamente en formato normalizado")

except Exception as e:
    print("Error en la ingesta de los datos de Barcelona:", e)
