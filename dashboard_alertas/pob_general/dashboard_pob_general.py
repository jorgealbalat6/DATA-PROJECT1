import streamlit as st
import plotly.express as px
import pandas as pd
from confluent_kafka import Consumer
import json
import time
import os

# --- 1. CONFIGURACIÓN ---
st.set_page_config(page_title="Mapa Calidad Aire", layout="wide", page_icon="🌍")
st.title("🌍 Mapa de Calidad del Aire en Tiempo Real")

# --- 2. LÍMITES (Para colorear el mapa) ---
LIMITES = {
    "NO2": 200, "PM2.5": 25, "PM10": 50, "O3": 180, "SO2": 350, "CO": 10
}

# --- 3. KAFKA ---
kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
conf = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'dashboard_mapa_final_v1',
    'auto.offset.reset': 'earliest'
}

if 'consumer' not in st.session_state:
    st.session_state.consumer = Consumer(conf)
    st.session_state.consumer.subscribe(['datos_pob_general'])

# Diccionario para guardar el ÚLTIMO dato de cada estación
if 'map_data' not in st.session_state:
    st.session_state.map_data = {}

# --- 4. FILTROS ---
st.sidebar.header("Filtros")
ciudad_filter = st.sidebar.radio("Ciudad", ["Madrid", "Barcelona"])
contaminante_filter = st.sidebar.selectbox("Contaminante", ["NO2", "PM10", "PM2.5", "O3", "SO2", "CO"])

# --- 5. LÓGICA DE COLORES ---
def get_color(valor, contaminante):
    limite = LIMITES.get(contaminante, 100)
    if valor < limite * 0.5: return "🟢 Bueno", "#2ecc71"
    elif valor < limite: return "🟠 Regular", "#f39c12"
    else: return "🔴 Malo", "#e74c3c"

# --- 6. BUCLE PRINCIPAL ---
map_placeholder = st.empty()

try:
    while True:
        # Leer lote de mensajes
        msgs = st.session_state.consumer.consume(num_messages=100, timeout=0.1)
        
        for msg in msgs:
            if msg is None or msg.error(): continue
            try:
                d = json.loads(msg.value().decode('utf-8'))
                
                # Normalizar nombres (Por si DBT manda nombres largos)
                indi = d.get('indicador') or d.get('contaminante') or d.get('magnitud')
                if "Nitrógeno" in str(indi): indi = "NO2"
                if "Partículas < 10" in str(indi): indi = "PM10"
                if "Partículas < 2.5" in str(indi): indi = "PM2.5"
                if "Ozono" in str(indi): indi = "O3"

                # Guardamos en el estado (Clave única: Estación + Contaminante)
                key = f"{d.get('estacion')}_{indi}"
                
                # Importante: Leer latitud/longitud
                st.session_state.map_data[key] = {
                    "estacion": d.get('estacion'),
                    "municipio": d.get('municipio'),
                    "contaminante": indi,
                    "valor": float(d.get('valor', 0)),
                    "lat": float(d.get('latitud', 0)), # Ojo: Tu mart dice 'latitud'
                    "lon": float(d.get('longitud', 0)) # Ojo: Tu mart dice 'longitud'
                }
            except Exception:
                pass

        # Preparar DataFrame para el mapa
        if st.session_state.map_data:
            df = pd.DataFrame(st.session_state.map_data.values())
            
            # Filtrar
            if ciudad_filter == "Madrid":
                df_show = df[df['municipio'].str.contains("Madrid", na=False)]
                zoom, center = 10, {"lat": 40.4168, "lon": -3.7038}
            else:
                df_show = df[df['municipio'].str.contains("Barcelona", na=False)]
                zoom, center = 11, {"lat": 41.3851, "lon": 2.1734}
            
            df_show = df_show[df_show['contaminante'] == contaminante_filter]

            if not df_show.empty:
                # Calcular colores
                df_show[['estado', 'color']] = df_show.apply(
                    lambda x: pd.Series(get_color(x['valor'], x['contaminante'])), axis=1
                )

                # Pintar mapa
                fig = px.scatter_mapbox(
                    df_show, lat="lat", lon="lon",
                    color="estado", size="valor", size_max=20,
                    color_discrete_map={"🟢 Bueno": "#2ecc71", "🟠 Regular": "#f39c12", "🔴 Malo": "#e74c3c"},
                    zoom=zoom, center=center, height=600,
                    hover_name="estacion", hover_data={"valor": True, "lat": False, "lon": False}
                )
                fig.update_layout(mapbox_style="carto-positron", margin={"r":0,"t":0,"l":0,"b":0})
                map_placeholder.plotly_chart(fig, use_container_width=True)
            else:
                map_placeholder.info(f"No hay datos de {contaminante_filter} para {ciudad_filter}...")
        
        time.sleep(1)

except KeyboardInterrupt:
    pass