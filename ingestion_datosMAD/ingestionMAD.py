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
    df_enero_2018 = pd.read_csv("ene_mo18.csv", sep=";")
    df_febrero_2018 = pd.read_csv("feb_mo18.csv", sep=";")
    df_marzo_2018 = pd.read_csv("mar_mo18.csv", sep=";")
    df_abril_2018 = pd.read_csv("abr_mo18.csv", sep=";")
    df_mayo_2018 = pd.read_csv("may_mo18.csv", sep=";")
    df_junio_2018 = pd.read_csv("jun_mo18.csv", sep=";")
    df_julio_2018 = pd.read_csv("jul_mo18.csv", sep=";")
    df_agosto_2018 = pd.read_csv("ago_mo18.csv", sep=";")
    df_septiembre_2018 = pd.read_csv("sep_mo18.csv", sep=";")
    df_octubre_2018 = pd.read_csv("oct_mo18.csv", sep=";")
    df_noviembre_2018 = pd.read_csv("nov_mo18.csv", sep=";")
    df_diciembre_2018 = pd.read_csv("dic_mo18.csv", sep=";")
    print("Se han leido correctamente los csv de 2018")
except Exception as e:
    print("Error leyendo el csv de 2018:", e)

try:
    df_enero_2019 = pd.read_csv("ene_mo19.csv", sep=";")
    df_febrero_2019 = pd.read_csv("feb_mo19.csv", sep=";")
    df_marzo_2019 = pd.read_csv("mar_mo19.csv", sep=";")
    df_abril_2019 = pd.read_csv("abr_mo19.csv", sep=";")
    df_mayo_2019 = pd.read_csv("may_mo19.csv", sep=";")
    df_junio_2019 = pd.read_csv("jun_mo19.csv", sep=";")
    df_julio_2019 = pd.read_csv("jul_mo19.csv", sep=";")
    df_agosto_2019 = pd.read_csv("ago_mo19.csv", sep=";")
    df_septiembre_2019 = pd.read_csv("sep_mo19.csv", sep=";")
    df_octubre_2019 = pd.read_csv("oct_mo19.csv", sep=";")
    df_noviembre_2019 = pd.read_csv("nov_mo19.csv", sep=";")
    df_diciembre_2019 = pd.read_csv("dic_mo19.csv", sep=";")
    print("Se han leido correctamente los csv de 2019")
except Exception as e:
    print("Error leyendo el csv de 2019:", e)

try:
    df_enero_2020 = pd.read_csv("ene_mo20.csv", sep=";")
    df_febrero_2020 = pd.read_csv("feb_mo20.csv", sep=";")
    df_marzo_2020 = pd.read_csv("mar_mo20.csv", sep=";")
    df_abril_2020 = pd.read_csv("abr_mo20.csv", sep=";")
    df_mayo_2020 = pd.read_csv("may_mo20.csv", sep=";")
    df_junio_2020 = pd.read_csv("jun_mo20.csv", sep=";")
    df_julio_2020 = pd.read_csv("jul_mo20.csv", sep=";")
    df_agosto_2020 = pd.read_csv("ago_mo20.csv", sep=";")
    df_septiembre_2020 = pd.read_csv("sep_mo20.csv", sep=";")
    df_octubre_2020 = pd.read_csv("oct_mo20.csv", sep=";")
    df_noviembre_2020 = pd.read_csv("nov_mo20.csv", sep=";")
    df_diciembre_2020 = pd.read_csv("dic_mo20.csv", sep=";")
    print("Se han leido correctamente los csv de 2020")
except Exception as e:
    print("Error leyendo el csv de 2020:", e)

try:
    df_enero_2021 = pd.read_csv("ene_mo21.csv", sep=";")
    df_febrero_2021 = pd.read_csv("feb_mo21.csv", sep=";")
    df_marzo_2021 = pd.read_csv("mar_mo21.csv", sep=";")
    df_abril_2021 = pd.read_csv("abr_mo21.csv", sep=";")
    df_mayo_2021 = pd.read_csv("may_mo21.csv", sep=";")
    df_junio_2021 = pd.read_csv("jun_mo21.csv", sep=";")
    df_julio_2021 = pd.read_csv("jul_mo21.csv", sep=";")
    df_agosto_2021 = pd.read_csv("ago_mo21.csv", sep=";")
    df_septiembre_2021 = pd.read_csv("sep_mo21.csv", sep=";")
    df_octubre_2021 = pd.read_csv("oct_mo21.csv", sep=";")
    df_noviembre_2021 = pd.read_csv("nov_mo21.csv", sep=";")
    df_diciembre_2021 = pd.read_csv("dic_mo21.csv", sep=";")
    print("Se han leido correctamente los csv de 2021")
except Exception as e:
    print("Error leyendo el csv de 2021:", e)

try:
    df_enero_2022 = pd.read_csv("ene_mo22.csv", sep=";")
    df_febrero_2022 = pd.read_csv("feb_mo22.csv", sep=";")
    df_marzo_2022 = pd.read_csv("mar_mo22.csv", sep=";")
    df_abril_2022 = pd.read_csv("abr_mo22.csv", sep=";")
    df_mayo_2022 = pd.read_csv("may_mo22.csv", sep=";")
    df_junio_2022 = pd.read_csv("jun_mo22.csv", sep=";")
    df_julio_2022 = pd.read_csv("jul_mo22.csv", sep=";")
    df_agosto_2022 = pd.read_csv("ago_mo22.csv", sep=";")
    df_septiembre_2022 = pd.read_csv("sep_mo22.csv", sep=";")
    df_octubre_2022 = pd.read_csv("oct_mo22.csv", sep=";")
    df_noviembre_2022 = pd.read_csv("nov_mo22.csv", sep=";")
    df_diciembre_2022 = pd.read_csv("dic_mo22.csv", sep=";")
    print("Se han leido correctamente los csv de 2022")
except Exception as e:
    print("Error leyendo el csv de 2022:", e)

try:
    df_enero_2023 = pd.read_csv("ene_mo23.csv", sep=";")
    df_febrero_2023 = pd.read_csv("feb_mo23.csv", sep=";")
    df_marzo_2023 = pd.read_csv("mar_mo23.csv", sep=";")
    df_abril_2023 = pd.read_csv("abr_mo23.csv", sep=";")
    df_mayo_2023 = pd.read_csv("may_mo23.csv", sep=";")
    df_junio_2023 = pd.read_csv("jun_mo23.csv", sep=";")
    df_julio_2023 = pd.read_csv("jul_mo23.csv", sep=";")
    df_agosto_2023 = pd.read_csv("ago_mo23.csv", sep=";")
    df_septiembre_2023 = pd.read_csv("sep_mo23.csv", sep=";")
    df_octubre_2023 = pd.read_csv("oct_mo23.csv", sep=";")
    df_noviembre_2023 = pd.read_csv("nov_mo23.csv", sep=";")
    df_diciembre_2023 = pd.read_csv("dic_mo23.csv", sep=";")
    print("Se han leido correctamente los csv de 2023")
except Exception as e:
    print("Error leyendo el csv de 2023:", e)

try:
    df_enero_2024 = pd.read_csv("ene_mo24.csv", sep=";")
    df_febrero_2024 = pd.read_csv("feb_mo24.csv", sep=";")
    df_marzo_2024 = pd.read_csv("mar_mo24.csv", sep=";")
    df_abril_2024 = pd.read_csv("abr_mo24.csv", sep=";")
    df_mayo_2024 = pd.read_csv("may_mo24.csv", sep=";")
    df_junio_2024 = pd.read_csv("jun_mo24.csv", sep=";")
    df_julio_2024 = pd.read_csv("jul_mo24.csv", sep=";")
    df_agosto_2024 = pd.read_csv("ago_mo24.csv", sep=";")
    df_septiembre_2024 = pd.read_csv("sep_mo24.csv", sep=";")
    df_octubre_2024 = pd.read_csv("oct_mo24.csv", sep=";")
    df_noviembre_2024 = pd.read_csv("nov_mo24.csv", sep=";")
    df_diciembre_2024 = pd.read_csv("dic_mo24.csv", sep=";")
    print("Se han leido correctamente los csv de 2024")
except Exception as e:
    print("Error leyendo el csv de 2024:", e)

try: 
    df_2025 = pd.read_csv("CALIDAD_AIRE_HORARIOS20251013.csv", sep=";")
    print("Se ha leido correctamente el csv de 2025")
except Exception as e:
    print("Error leyendo el csv de 2025:", e)


try: 
    lista_df = [
        df_enero_2018,
        df_febrero_2018,
        df_marzo_2018,
        df_abril_2018,
        df_mayo_2018,
        df_junio_2018,
        df_julio_2018,
        df_agosto_2018,
        df_septiembre_2018,
        df_octubre_2018,
        df_noviembre_2018,
        df_diciembre_2018,
        df_enero_2019,
        df_febrero_2019,
        df_marzo_2019,
        df_abril_2019,
        df_mayo_2019,
        df_junio_2019,
        df_julio_2019,
        df_agosto_2019,
        df_septiembre_2019,
        df_octubre_2019,
        df_noviembre_2019,
        df_diciembre_2019,
        df_enero_2020,
        df_febrero_2020,
        df_marzo_2020,
        df_abril_2020,
        df_mayo_2020,
        df_junio_2020,
        df_julio_2020,
        df_agosto_2020,
        df_septiembre_2020,
        df_octubre_2020,
        df_noviembre_2020,
        df_diciembre_2020,
        df_enero_2021,
        df_febrero_2021,
        df_marzo_2021,
        df_abril_2021,
        df_mayo_2021,
        df_junio_2021,
        df_julio_2021,
        df_agosto_2021,
        df_septiembre_2021,
        df_octubre_2021,
        df_noviembre_2021,
        df_diciembre_2021,
        df_enero_2022,
        df_febrero_2022,
        df_marzo_2022,
        df_abril_2022,
        df_mayo_2022,
        df_junio_2022,
        df_julio_2022,
        df_agosto_2022,
        df_septiembre_2022,
        df_octubre_2022,
        df_noviembre_2022,
        df_diciembre_2022,
        df_enero_2023,
        df_febrero_2023,
        df_marzo_2023,
        df_abril_2023,
        df_mayo_2023,
        df_junio_2023,
        df_julio_2023,
        df_agosto_2023,
        df_septiembre_2023,
        df_octubre_2023,
        df_noviembre_2023,
        df_diciembre_2023,
        df_enero_2024,
        df_febrero_2024,
        df_marzo_2024,
        df_abril_2024,
        df_mayo_2024,
        df_junio_2024,
        df_julio_2024,
        df_agosto_2024,
        df_septiembre_2024,
        df_octubre_2024,
        df_noviembre_2024,
        df_diciembre_2024,
        df_2025
    ]
    df_total = pd.concat(lista_df, ignore_index=True)
    print("Se han unido todos los csv")
except Exception as e: 
    print("Error uniendo los csv:", e)

try: 
    if "PROVINCIA" in df_total.columns:
        df_total = df_total.drop(columns=["PROVINCIA"])
    print("Se ha eliminado la columna correctamente")
except Exception as e: 
    print("Error eliminando columna:", e)

try: 
    print("Transformando datos con Pandas...")
    df_long = pd.wide_to_long(
        df_total, 
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