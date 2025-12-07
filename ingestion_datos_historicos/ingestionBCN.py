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
    df_historico_BCN = df_historico_BCN[df_historico_BCN['NOM MUNICIPI']=='Barcelona']
    print("Se han eliminado correctamente los datos que no pertenecen a Barcelona")
except Exception as e:
    print("Error al eliminar los datos que no son de Barcelona:", e)

try:
    if "CODI EOI" in df_historico_BCN:
        df_historico_BCN = df_historico_BCN.drop(columns=["CODI EOI"])
    print("Se ha eliminado la columna CODI EOI correctamente")
except Exception as e:
    print("Error eliminando la columna CODI EOI:", e)

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
    print("Error eliminando la columna Georeferència", e)

try:
    print("Transformando datos con Pandas...")
    # Melt (unpivot) the daily columns D01-D31
    df_melted = df_historico_BCN.melt(
        id_vars=['NOM ESTACIÓ', 'NOM MUNICIPI', 'MAGNITUD', 'ANY', 'MES', 'LATITUD', 'LONGITUD'],
        value_vars=[f'D{i:02d}' for i in range(1, 32)],
        var_name='DIA_STR',
        value_name='VALOR'
    )

    # Convert DIA_STR (e.g., 'D01') to integer day
    df_melted['DIA'] = df_melted['DIA_STR'].str.replace('D', '').astype(int)

    # Filter out rows with missing values (NaN in VALOR)
    df_final = df_melted.dropna(subset=['VALOR']).copy()

    # Add missing columns required by the DB schema
    df_final['HORA'] = 0
    df_final['VALIDACION'] = 'V'
    
    # MUNICIPIO: Hardcoded to 8019 (Barcelona) as CODI INE is dropped
    df_final['MUNICIPIO'] = 8019 
    
    # PUNTO_MUESTREO: CODI EOI is dropped. Setting to NULL.
    df_final['PUNTO_MUESTREO'] = None 

    # Rename columns to match target schema
    df_final = df_final.rename(columns={
        'NOM ESTACIÓ': 'ESTACION',
        'ANY': 'ANO',
        'LATITUD': 'LAT',
        'LONGITUD': 'LON'
    })

    # Select and order final columns
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
    if 'connection' in locals() and connection:
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
        print("Datos históricos ingestados correctamente en formato normalizado")
    else:
        print("No hay conexión a la base de datos establecida.")

except Exception as e:
    print("Error en la ingesta de los datos de Barcelona:", e)
