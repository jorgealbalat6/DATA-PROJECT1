import streamlit as st
import plotly.express as px
import pandas as pd
from confluent_kafka import Consumer
import json
import time

# --- 1. CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    page_title="Monitor Madrid - Pob. General",
    layout="wide",
    page_icon="🏙️"
)

st.title("🏙️ Calidad del Aire: Población General")
st.markdown("_Datos en tiempo real desde Kafka (Topic: datos_pob_general)_")

# --- 2. LÍMITES LEGALES (Los mismos que tienes en alertas.py) ---
# Los usamos para pintar una línea roja de referencia en el gráfico
LIMITES = {
    "Dióxido de Nitrógeno (NO2)": 200, # Ojo, 200 es el horario, 400 alerta. Ponemos referencia visual.
    "Partículas < 2.5 µm (PM2.5)": 25,
    "Partículas < 10 µm (PM10)": 50,
    "Ozono (O3)": 180,
    "Dióxido de Azufre (SO2)": 350
}

# --- 3. CONFIGURACIÓN KAFKA CONSUMER ---
conf = {
    'bootstrap.servers': 'localhost:9092', # Usa localhost si corres streamlit desde fuera de Docker
    'group.id': 'dashboard_streamlit_gen_v1',
    'auto.offset.reset': 'earliest' # Para leer los datos históricos que cargue tu productor
}

# Inicializamos el consumidor solo una vez
if 'consumer' not in st.session_state:
    st.session_state.consumer = Consumer(conf)
    st.session_state.consumer.subscribe(['datos_pob_general'])

# --- 4. ESTADO DE DATOS (Session State) ---
# Aquí acumulamos todos los mensajes que llegan de Kafka
if 'df_aire' not in st.session_state:
    st.session_state.df_aire = pd.DataFrame(columns=['estacion', 'contaminante', 'fecha', 'valor'])

# --- 5. SIDEBAR (FILTROS) ---
st.sidebar.header("Filtros de Visualización")

# Selector de Contaminante
# Truco: Si ya tenemos datos, sacamos la lista de contaminantes únicos recibidos
lista_contaminantes = st.session_state.df_aire['contaminante'].unique().tolist()
if not lista_contaminantes:
    lista_contaminantes = ["Esperando datos..."]

contaminante_seleccionado = st.sidebar.selectbox(
    "Selecciona Contaminante",
    options=lista_contaminantes,
    index=0
)

# --- 6. HUECOS PARA GRÁFICOS ---
# Creamos contenedores vacíos que actualizaremos en el bucle
kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
grafico_principal = st.empty()
tabla_datos = st.empty()

# --- 7. BUCLE DE LECTURA Y ACTUALIZACIÓN ---
try:
    # Botón para detener si quieres
    stop_btn = st.sidebar.button("Detener Monitorización")
    
    while not stop_btn:
        # A. LEER KAFKA
        msg = st.session_state.consumer.poll(0.1)

        nuevos_datos = []
        
        # B. PROCESAR MENSAJE (Si existe)
        if msg is not None and not msg.error():
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Extraemos lo que nos interesa para el gráfico
                nuevos_datos.append({
                    'estacion': data['estacion'],
                    'contaminante': data['contaminante'],
                    'fecha': data['fecha'], # Asegúrate que viene en formato ISO o string ordenable
                    'valor': float(data['valor'])
                })
            except Exception as e:
                print(f"Error parseando mensaje: {e}")

        # C. ACTUALIZAR DATAFRAME GLOBAL
        if nuevos_datos:
            nuevo_df = pd.DataFrame(nuevos_datos)
            # Concatenamos con lo que ya teníamos
            st.session_state.df_aire = pd.concat([st.session_state.df_aire, nuevo_df], ignore_index=True)
            # Opcional: Mantener solo los últimos 1000 registros para no explotar la RAM
            if len(st.session_state.df_aire) > 2000:
                st.session_state.df_aire = st.session_state.df_aire.tail(2000)

        # D. FILTRAR Y PINTAR (Solo si hay datos del contaminante seleccionado)
        df_plot = st.session_state.df_aire[
            st.session_state.df_aire['contaminante'] == contaminante_seleccionado
        ]

        if not df_plot.empty:
            # Ordenamos por fecha para que la línea salga bien
            df_plot = df_plot.sort_values('fecha')

            # --- GRÁFICO PLOTLY ---
            fig = px.line(
                df_plot, 
                x='fecha', 
                y='valor', 
                color='estacion', # ¡Una línea por cada estación automáticamente!
                title=f"Evolución de {contaminante_seleccionado}",
                template="plotly_dark",
                markers=True
            )

            # Añadir línea de límite legal (si existe en tu diccionario LIMITES)
            limite = LIMITES.get(contaminante_seleccionado)
            if limite:
                fig.add_hline(y=limite, line_dash="dash", line_color="red", annotation_text="Límite Alerta")

            # Actualizar el gráfico en pantalla
            grafico_principal.plotly_chart(fig, use_container_width=True)

            # --- KPIs (Ejemplo: Valor máximo detectado hoy) ---
            max_val = df_plot['valor'].max()
            estacion_max = df_plot.loc[df_plot['valor'].idxmax()]['estacion']
            
            kpi_col1.metric("Máximo Registrado", f"{max_val}", delta="µg/m³")
            kpi_col2.metric("Estación con pico", estacion_max)
            kpi_col3.metric("Registros totales", len(df_plot))

            # --- TABLA ---
            with tabla_datos.expander("Ver últimos datos brutos"):
                st.dataframe(df_plot.tail(10))

        # Pequeña pausa para no saturar el procesador
        time.sleep(0.1)

except KeyboardInterrupt:
    pass