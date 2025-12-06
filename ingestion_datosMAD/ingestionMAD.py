import psycopg
import pandas as pd
import io 
import os, time
import gc # Importante: Para liberar memoria manualmente

# --- 1. CONEXIÓN A LA BASE DE DATOS ---
conn = None
for i in range(10):
    try:
        url = os.getenv("DATABASE_URL")
        connection = psycopg.connect(url)
        cur = connection.cursor()
        conn = connection
        print("BD conectada con éxito")
        break
    except Exception as e :
        print("Error conectando a la BD:", e)
        time.sleep(2)

if not conn:
    print("No se pudo conectar a la BD tras varios intentos. Saliendo.")
    exit()

# --- 2. FUNCIÓN PARA PROCESAR Y GUARDAR (REUTILIZABLE) ---
def procesar_y_guardar(df_sucio, anio):
    if df_sucio.empty:
        print(f"Advertencia: DataFrame del año {anio} está vacío.")
        return

    try:
        # Limpieza inicial
        if "PROVINCIA" in df_sucio.columns:
            df_sucio = df_sucio.drop(columns=["PROVINCIA"])

        print(f"[{anio}] Transformando datos (Wide -> Long)...")
        
        # Wide to Long (La operación pesada)
        df_long = pd.wide_to_long(
            df_sucio, 
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
        
        # Filtramos y limpiamos
        df_final = df_long[columnas_finales].dropna(subset=['VALOR'])
        
        print(f"[{anio}] Filas listas para insertar: {len(df_final)}")

        # Ingesta a SQL
        buffer = io.StringIO()
        df_final.to_csv(buffer, index=False, header=False, sep=',')
        buffer.seek(0)
        
        sql = """
        COPY calidad_aire (
            MUNICIPIO, ESTACION, MAGNITUD, PUNTO_MUESTREO, 
            ANO, MES, DIA, HORA, VALOR, VALIDACION
        )
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',')
        """
        
        with cur.copy(sql) as copy:
            copy.write(buffer.getvalue())
        
        conn.commit()
        print(f"✅ [{anio}] Datos guardados correctamente en BD.")
        
    except Exception as e:
        print(f"❌ Error procesando el año {anio}: {e}")
        conn.rollback()

# --- 3. PROCESAMIENTO AÑO POR AÑO ---

# Definimos los archivos por año para iterar limpio
archivos_por_anio = {
    2018: ["ene_mo18.csv", "feb_mo18.csv", "mar_mo18.csv", "abr_mo18.csv", "may_mo18.csv", "jun_mo18.csv", "jul_mo18.csv", "ago_mo18.csv", "sep_mo18.csv", "oct_mo18.csv", "nov_mo18.csv", "dic_mo18.csv"],
    2019: ["ene_mo19.csv", "feb_mo19.csv", "mar_mo19.csv", "abr_mo19.csv", "may_mo19.csv", "jun_mo19.csv", "jul_mo19.csv", "ago_mo19.csv", "sep_mo19.csv", "oct_mo19.csv", "nov_mo19.csv", "dic_mo19.csv"],
    2020: ["ene_mo20.csv", "feb_mo20.csv", "mar_mo20.csv", "abr_mo20.csv", "may_mo20.csv", "jun_mo20.csv", "jul_mo20.csv", "ago_mo20.csv", "sep_mo20.csv", "oct_mo20.csv", "nov_mo20.csv", "dic_mo20.csv"],
    2021: ["ene_mo21.csv", "feb_mo21.csv", "mar_mo21.csv", "abr_mo21.csv", "may_mo21.csv", "jun_mo21.csv", "jul_mo21.csv", "ago_mo21.csv", "sep_mo21.csv", "oct_mo21.csv", "nov_mo21.csv", "dic_mo21.csv"],
    2022: ["ene_mo22.csv", "feb_mo22.csv", "mar_mo22.csv", "abr_mo22.csv", "may_mo22.csv", "jun_mo22.csv", "jul_mo22.csv", "ago_mo22.csv", "sep_mo22.csv", "oct_mo22.csv", "nov_mo22.csv", "dic_mo22.csv"],
    2023: ["ene_mo23.csv", "feb_mo23.csv", "mar_mo23.csv", "abr_mo23.csv", "may_mo23.csv", "jun_mo23.csv", "jul_mo23.csv", "ago_mo23.csv", "sep_mo23.csv", "oct_mo23.csv", "nov_mo23.csv", "dic_mo23.csv"],
    2024: ["ene_mo24.csv", "feb_mo24.csv", "mar_mo24.csv", "abr_mo24.csv", "may_mo24.csv", "jun_mo24.csv", "jul_mo24.csv", "ago_mo24.csv", "sep_mo24.csv", "oct_mo24.csv", "nov_mo24.csv", "dic_mo24.csv"],
}

# Procesar 2018 - 2024
for anio, lista_archivos in archivos_por_anio.items():
    print(f"--- Iniciando proceso Año {anio} ---")
    dfs_anio = []
    for archivo in lista_archivos:
        try:
            # Asumimos que los archivos están en la misma carpeta
            df = pd.read_csv(archivo, sep=";")
            dfs_anio.append(df)
        except Exception as e:
            print(f"Error leyendo {archivo}: {e}")
    
    if dfs_anio:
        # Unimos solo este año
        df_total_anio = pd.concat(dfs_anio, ignore_index=True)
        # Procesamos y guardamos (Esto inserta en BD)
        procesar_y_guardar(df_total_anio, anio)
        
        # LIMPIEZA DE MEMORIA CRUCIAL
        del df_total_anio
        del dfs_anio
        gc.collect() # Forzamos al recolector de basura de Python
    else:
        print(f"No se encontraron datos para el año {anio}")

# Procesar 2025 (Caso especial un solo archivo)
print("--- Iniciando proceso Año 2025 ---")
try:
    df_2025 = pd.read_csv("CALIDAD_AIRE_HORARIOS20251013.csv", sep=";")
    procesar_y_guardar(df_2025, 2025)
    del df_2025
    gc.collect()
except Exception as e:
    print(f"Error leyendo csv 2025: {e}")

print("🏁 Ingestión histórica finalizada.")