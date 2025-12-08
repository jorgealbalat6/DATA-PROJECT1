from dash import Dash, html, dcc, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
from confluent_kafka import Consumer, KafkaError
import json
import threading
from collections import deque
from datetime import datetime
import time
import os

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
TOPIC_DATOS = 'datos_pob_general'
TOPIC_ALERTAS = 'alertas_pob_general'

# Límites de calidad del aire desde marts_CalidadAire_general (µg/m³)
LIMITES_CALIDAD = {
    'PM2.5': 18,
    'PM10': 35,
    'NO2': 65,
    'O3': 115,
    'SO2': 140,
    'CO': 8
}

# Colores para cada contaminante
COLORES_MAGNITUDES = {
    'PM2.5': '#FF6B6B',
    'PM10': '#4ECDC4',
    'NO2': '#45B7D1',
    'O3': '#96CEB4',
    'SO2': '#FFEAA7',
    'CO': '#DFE6E9'
}

# Almacenamiento de datos en memoria
class DataStore:
    def __init__(self, maxlen=500):
        self.datos = deque(maxlen=maxlen)
        self.alertas = deque(maxlen=100)
        self.lock = threading.Lock()
    
    def add_dato(self, dato):
        with self.lock:
            self.datos.append(dato)
    
    def add_alerta(self, alerta):
        with self.lock:
            self.alertas.append(alerta)
    
    def get_datos_df(self):
        with self.lock:
            if not self.datos:
                return pd.DataFrame()
            return pd.DataFrame(list(self.datos))
    
    def get_alertas_df(self):
        with self.lock:
            if not self.alertas:
                return pd.DataFrame()
            return pd.DataFrame(list(self.alertas))

# Instancia global del almacén de datos
data_store = DataStore()

# Función para normalizar nombres de columnas
def normalize_dataframe(df):
    """Normaliza nombres de columnas para compatibilidad"""
    if df.empty:
        return df
    
    df = df.copy()
    
    # Renombrar 'indicador' a 'magnitud' si existe
    if 'indicador' in df.columns and 'magnitud' not in df.columns:
        df.rename(columns={'indicador': 'magnitud'}, inplace=True)
    
    return df

# Función para consumir datos de Kafka
def consume_datos():
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'dashboard-datos-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC_DATOS])
    
    print(f"Consumidor de datos iniciado para tópico: {TOPIC_DATOS}")
    
    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error en consumidor de datos: {msg.error()}")
                    continue
            
            dato = json.loads(msg.value().decode('utf-8'))
            dato['timestamp'] = datetime.now().isoformat()
            data_store.add_dato(dato)
            
        except Exception as e:
            print(f"Error procesando mensaje de datos: {e}")
            time.sleep(5)

# Función para consumir alertas de Kafka
def consume_alertas():
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'dashboard-alertas-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC_ALERTAS])
    
    print(f"Consumidor de alertas iniciado para tópico: {TOPIC_ALERTAS}")
    
    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error en consumidor de alertas: {msg.error()}")
                    continue
            
            alerta = json.loads(msg.value().decode('utf-8'))
            alerta['timestamp'] = datetime.now().isoformat()
            data_store.add_alerta(alerta)
            
        except Exception as e:
            print(f"Error procesando mensaje de alertas: {e}")
            time.sleep(5)

# Iniciar consumidores en hilos separados
thread_datos = threading.Thread(target=consume_datos, daemon=True)
thread_alertas = threading.Thread(target=consume_alertas, daemon=True)
thread_datos.start()
thread_alertas.start()

# Función para determinar el color basado en el valor y límite
def get_color_by_level(valor, limite):
    if valor < limite * 0.5:
        return '#2ecc71'  # Verde - Bueno
    elif valor < limite * 0.75:
        return '#f39c12'  # Naranja - Moderado
    elif valor < limite:
        return '#e67e22'  # Naranja oscuro
    else:
        return '#e74c3c'  # Rojo - No saludable

# Inicializar la aplicación Dash
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout del dashboard
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("🌍 Dashboard Calidad del Aire - Población General", 
                   className="text-center text-primary mb-2 mt-3"),
            html.H5(id='last-update', className="text-center text-muted mb-3")
        ])
    ]),
    
    # Selector de Ciudad
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Label("Seleccionar Ciudad:", className="fw-bold mb-2"),
                    dbc.RadioItems(
                        id='city-selector',
                        options=[
                            {'label': '🏙️ Madrid', 'value': 'Madrid'},
                            {'label': '🏙️ Barcelona', 'value': 'Barcelona'},
                            {'label': '🌐 Todas', 'value': 'Todas'}
                        ],
                        value='Madrid',
                        inline=True,
                        className='mb-2'
                    ),
                    html.Label("Seleccionar Contaminante:", className="fw-bold mb-2 mt-3"),
                    dbc.Select(
                        id='map-pollutant-selector',
                        options=[
                            {'label': 'PM2.5', 'value': 'PM2.5'},
                            {'label': 'PM10', 'value': 'PM10'},
                            {'label': 'NO2', 'value': 'NO2'},
                            {'label': 'O3', 'value': 'O3'},
                            {'label': 'SO2', 'value': 'SO2'},
                            {'label': 'CO', 'value': 'CO'}
                        ],
                        value='PM2.5'
                    )
                ])
            ])
        ], md=12, className="mb-3")
    ]),
    
    # KPI Cards
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("PM2.5", className="card-subtitle text-muted"),
                    html.H3(id='kpi-pm25', className="card-title"),
                    html.Small(f"Límite: {LIMITES_CALIDAD['PM2.5']} µg/m³", className="text-muted")
                ])
            ], color="light", className="mb-3")
        ], md=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("PM10", className="card-subtitle text-muted"),
                    html.H3(id='kpi-pm10', className="card-title"),
                    html.Small(f"Límite: {LIMITES_CALIDAD['PM10']} µg/m³", className="text-muted")
                ])
            ], color="light", className="mb-3")
        ], md=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("NO2", className="card-subtitle text-muted"),
                    html.H3(id='kpi-no2', className="card-title"),
                    html.Small(f"Límite: {LIMITES_CALIDAD['NO2']} µg/m³", className="text-muted")
                ])
            ], color="light", className="mb-3")
        ], md=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("O3", className="card-subtitle text-muted"),
                    html.H3(id='kpi-o3', className="card-title"),
                    html.Small(f"Límite: {LIMITES_CALIDAD['O3']} µg/m³", className="text-muted")
                ])
            ], color="light", className="mb-3")
        ], md=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("SO2", className="card-subtitle text-muted"),
                    html.H3(id='kpi-so2', className="card-title"),
                    html.Small(f"Límite: {LIMITES_CALIDAD['SO2']} µg/m³", className="text-muted")
                ])
            ], color="light", className="mb-3")
        ], md=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("CO", className="card-subtitle text-muted"),
                    html.H3(id='kpi-co', className="card-title"),
                    html.Small(f"Límite: {LIMITES_CALIDAD['CO']} µg/m³", className="text-muted")
                ])
            ], color="light", className="mb-3")
        ], md=2),
    ]),
    
    # Mapa
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("🗺️ Mapa de Calidad del Aire")),
                dbc.CardBody([
                    dcc.Graph(id='map-graph')
                ])
            ])
        ], md=12, className="mb-3")
    ]),
    
    # Tabla de datos por estación
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("📋 Datos por Estación")),
                dbc.CardBody([
                    html.Div(id='station-data-table')
                ])
            ])
        ], md=12, className="mb-3")
    ]),
    
    # Alerts Panel
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H5("🚨 Alertas en Tiempo Real"),
                    dbc.Badge(id='alert-count', color="danger", className="ms-2")
                ]),
                dbc.CardBody([
                    html.Div(id='alerts-table')
                ])
            ], className="mb-3")
        ])
    ]),
    
    # Intervalo para actualización automática
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # 5 segundos
        n_intervals=0
    )
], fluid=True, style={'backgroundColor': '#f8f9fa'})

# Callbacks
@app.callback(
    [Output('last-update', 'children'),
     Output('kpi-pm25', 'children'),
     Output('kpi-pm10', 'children'),
     Output('kpi-no2', 'children'),
     Output('kpi-o3', 'children'),
     Output('kpi-so2', 'children'),
     Output('kpi-co', 'children'),
     Output('map-graph', 'figure'),
     Output('station-data-table', 'children'),
     Output('alerts-table', 'children'),
     Output('alert-count', 'children')],
    [Input('interval-component', 'n_intervals'),
     Input('map-pollutant-selector', 'value'),
     Input('city-selector', 'value')]
)
def update_dashboard(n, selected_pollutant, selected_city):
    df = data_store.get_datos_df()
    df_alertas = data_store.get_alertas_df()
    
    # Normalizar nombres de columnas
    df = normalize_dataframe(df)
    df_alertas = normalize_dataframe(df_alertas)
    
    # Filtrar por ciudad si es necesario
    if selected_city != 'Todas' and not df.empty and 'municipio' in df.columns:
        df = df[df['municipio'] == selected_city]
    
    # Última actualización
    last_update = f"Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Ciudad: {selected_city}"
    
    # KPIs - calcular promedios recientes por magnitud
    kpis = {}
    for magnitud in ['PM2.5', 'PM10', 'NO2', 'O3', 'SO2', 'CO']:
        if not df.empty and 'magnitud' in df.columns:
            datos_magnitud = df[df['magnitud'] == magnitud]
            if not datos_magnitud.empty and 'valor' in datos_magnitud.columns:
                valor_promedio = datos_magnitud['valor'].tail(20).mean()
                kpis[magnitud] = f"{valor_promedio:.1f}"
            else:
                kpis[magnitud] = "N/A"
        else:
            kpis[magnitud] = "N/A"
    
    # Mapa interactivo
    map_fig = go.Figure()
    
    if not df.empty and 'latitud' in df.columns and 'longitud' in df.columns and 'magnitud' in df.columns:
        # Filtrar por el contaminante seleccionado
        df_map = df[df['magnitud'] == selected_pollutant].copy()
        
        if not df_map.empty:
            # Obtener último valor por estación
            df_map = df_map.sort_values('timestamp').groupby('estacion').tail(1)
            
            # Filtrar filas con coordenadas válidas
            df_map = df_map.dropna(subset=['latitud', 'longitud'])
            
            if not df_map.empty:
                # Calcular colores basados en límites
                limite = LIMITES_CALIDAD.get(selected_pollutant, 100)
                df_map['color'] = df_map['valor'].apply(lambda x: get_color_by_level(x, limite))
                df_map['size'] = df_map['valor'].apply(lambda x: min(max(x/limite * 30, 15), 50))
                
                # Crear texto hover
                df_map['hover_text'] = df_map.apply(
                    lambda row: f"<b>{row.get('estacion', 'N/A')}</b><br>" +
                               f"{row.get('municipio', 'N/A')}<br>" +
                               f"{selected_pollutant}: {row['valor']:.1f} µg/m³<br>" +
                               f"Límite: {limite} µg/m³",
                    axis=1
                )
                
                map_fig.add_trace(go.Scattermapbox(
                    lat=df_map['latitud'],
                    lon=df_map['longitud'],
                    mode='markers',
                    marker=dict(
                        size=df_map['size'],
                        color=df_map['color'],
                        opacity=0.8
                    ),
                    text=df_map['hover_text'],
                    hoverinfo='text',
                    name=selected_pollutant
                ))
                
                # Centrar mapa
                center_lat = df_map['latitud'].mean()
                center_lon = df_map['longitud'].mean()
                
                map_fig.update_layout(
                    mapbox=dict(
                        style='open-street-map',
                        center=dict(lat=center_lat, lon=center_lon),
                        zoom=11
                    ),
                    showlegend=False,
                    height=500,
                    margin=dict(l=0, r=0, t=0, b=0)
                )
    
    # Si no hay datos válidos para el mapa
    if len(map_fig.data) == 0:
        # Centrar en Madrid o Barcelona según selección
        if selected_city == 'Barcelona':
            center_coords = (41.3851, 2.1734)
        else:
            center_coords = (40.4168, -3.7038)
        
        map_fig.update_layout(
            mapbox=dict(
                style='open-street-map',
                center=dict(lat=center_coords[0], lon=center_coords[1]),
                zoom=11
            ),
            height=500,
            margin=dict(l=0, r=0, t=0, b=0),
            annotations=[
                dict(
                    text=f"Esperando datos de {selected_city}...",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(size=20, color='gray')
                )
            ]
        )
    
    # Tabla de datos por estación
    station_table = html.P("No hay datos disponibles", className="text-muted text-center")
    
    if not df.empty and 'estacion' in df.columns and 'magnitud' in df.columns and 'valor' in df.columns:
        # Filtrar solo los contaminantes deseados
        contaminantes_deseados = ['PM2.5', 'PM10', 'NO2', 'O3', 'SO2', 'CO']
        df_filtered = df[df['magnitud'].isin(contaminantes_deseados)]
        
        if not df_filtered.empty:
            # Obtener últimos datos por estación y magnitud
            df_pivot = df_filtered.sort_values('timestamp').groupby(['estacion', 'magnitud']).tail(1)
            
            # Crear pivot table: estaciones en filas, magnitudes en columnas
            pivot_data = df_pivot.pivot_table(
                index='estacion',
                columns='magnitud',
                values='valor',
                aggfunc='last'
            ).reset_index()
            
            if not pivot_data.empty:
                # Reordenar columnas en el orden deseado
                cols_order = ['estacion'] + [c for c in contaminantes_deseados if c in pivot_data.columns]
                pivot_data = pivot_data[cols_order]
                
                # Formatear valores
                for col in pivot_data.columns:
                    if col != 'estacion':
                        pivot_data[col] = pivot_data[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
                
                # Crear columnas para la tabla
                columns = [{"name": col, "id": col} for col in pivot_data.columns]
                
                # Estilo condicional para resaltar valores que exceden límites
                style_data_conditional = [
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(248, 248, 248)'
                    }
                ]
                
                station_table = dash_table.DataTable(
                    data=pivot_data.to_dict('records'),
                    columns=columns,
                    style_cell={
                        'textAlign': 'center',
                        'fontSize': '14px',
                        'padding': '10px'
                    },
                    style_header={
                        'backgroundColor': '#007bff',
                        'color': 'white',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                    style_data_conditional=style_data_conditional,
                    page_size=15
                )
    
    # Tabla de alertas
    alerts_table = html.P("No hay alertas activas", className="text-muted text-center")
    alert_count = "0"
    
    if not df_alertas.empty:
        # Filtrar por ciudad si es necesario
        df_alertas_filtered = df_alertas
        if selected_city != 'Todas' and 'municipio' in df_alertas.columns:
            df_alertas_filtered = df_alertas[df_alertas['municipio'] == selected_city]
        
        if not df_alertas_filtered.empty:
            # Ordenar por timestamp descendente
            df_alertas_sorted = df_alertas_filtered.sort_values('timestamp', ascending=False).head(20)
            
            # Preparar datos para la tabla
            alerts_data = []
            for _, row in df_alertas_sorted.iterrows():
                alerts_data.append({
                    'Fecha': row.get('fecha', 'N/A'),
                    'Estación': row.get('estacion', 'N/A'),
                    'Municipio': row.get('municipio', 'N/A'),
                    'Magnitud': row.get('magnitud', 'N/A'),
                    'Valor': f"{row.get('valor', 0):.2f}",
                    'Límite': f"{row.get('limite', 0):.2f}",
                    'Mensaje': row.get('mensaje', 'N/A')
                })
            
            alerts_table = dash_table.DataTable(
                data=alerts_data,
                columns=[{"name": i, "id": i} for i in ['Fecha', 'Estación', 'Municipio', 'Magnitud', 'Valor', 'Límite', 'Mensaje']],
                style_cell={
                    'textAlign': 'left',
                    'fontSize': '12px',
                    'padding': '8px',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'maxWidth': 0
                },
                style_header={
                    'backgroundColor': '#dc3545',
                    'color': 'white',
                    'fontWeight': 'bold'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(248, 248, 248)'
                    },
                    {
                        'if': {'column_id': 'Mensaje'},
                        'fontWeight': 'bold',
                        'color': '#dc3545'
                    }
                ]
            )
            
            alert_count = str(len(df_alertas_filtered))
    
    return (last_update, kpis.get('PM2.5', 'N/A'), kpis.get('PM10', 'N/A'), 
            kpis.get('NO2', 'N/A'), kpis.get('O3', 'N/A'), kpis.get('SO2', 'N/A'), 
            kpis.get('CO', 'N/A'), map_fig, station_table, 
            alerts_table, alert_count)

if __name__ == '__main__':
    print("Iniciando Dashboard de Calidad del Aire...")
    print(f"Consumiendo datos de tópicos: {TOPIC_DATOS}, {TOPIC_ALERTAS}")
    print(f"Límites configurados desde marts_CalidadAire_general:")
    for k, v in LIMITES_CALIDAD.items():
        print(f"  {k}: {v} µg/m³")
    app.run_server(host='0.0.0.0', port=8050, debug=False)
