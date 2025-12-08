# DATA PROJECT 1 
# Sistema de Alertas de Calidad del Aire
Este sistema procesa datos de calidad del aire (históricos y en tiempo real), genera alertas para la población general y para la poblacion de riesgo, y muestra dashboards con la información para que sea entendible y facil de intepretar.

**Comando para ponerlo en marcha**

```bash
# Construir imágenes necesarias
docker compose --profile workers build

# Levantar todos los servicios
docker compose up
```

**Estructura breve del repositorio**

- `docker-compose.yml` — orquesta los servicios
- `API/` — API Flask para ingesta
- `dags/` — DAGs de Airflow (orquestación)
- `ingestion/` y `ingestion_datos_historicos/` — scripts de ingesta
- `dbt/` — transformaciones y modelos
- `dashboard_alertas/` — dashboards para ambos grupos
- `docker-init/crear_tablas.sql` — esquema inicial de la BD

---
## Proceso

1. **Ingesta de datos**  
   - CSVs históricos mediante scripts batch  
   - Datos en tiempo real a través de la API

2. **Almacenamiento en PostgreSQL**  
   - Los datos crudos (raw) se guardan en la base de datos 

3. **Transformaciones con dbt**  
   - Limpieza, estandarización y modelado  
   - Creación de tablas intermedias y modelos finales
   - se establecen limites para determinar que valores deben generar alertas  


4. **Mensajería y streaming con Kafka**  
   - Alertas según limites establecidos en dbt
   - Kafka distribuye mensajes a plotly

5. **Visualización en dashboards**  
   - Dashboards para población general y población de riesgo  
   - Consumen los mensajes creados en kafka
   - Presentan métricas, series temporales y alertas activas

---

## Arquitectura

- Orquestación: Airflow para programar y disparar pipelines.
- Ingesta: API Flask y scripts batch para históricos.
- Almacenamiento: PostgreSQL.
- Transformación: dbt.
- Streaming: Kafka (topics para datos y alertas).
- Visualización: Dashboards (Plotly) consumiendo los modelos.

---
## Justificación

- **API Flask**: Se eligió para la ingesta de datos en tiempo real para una mayor escalabilidad y que se puedan incorporar nuevas ciudades con mayor facilidad.

- **PostgreSQL**: Base de datos relacional utilizada para almacenar datos históricos y en tiempo real. Se eligió SQL porque la estructura de los datos es fija (variables de calidad del aire) y permite definir un esquema claro y consistente. Además facilita las transformaciones posteriores con dbt y la consulta eficiente para los dashboards.

- **dbt**: Herramienta de transformación y modelado de datos. Se implementó un esquema de **tres capas**:
  1. **Staging**: limpieza de datos raw y renombramiento de columnas
  2. **Intermediate**: limpieza de datos para un mejor entendimiento.  
  3. **Marts**: tablas finales separadas para **población general** y **población de riesgo**, listas para consumo por dashboards y alertas.  
  La elección de dbt se debe a su capacidad de versionar modelos SQL, automatizar pruebas de calidad.

- **Kafka**: Se utiliza para la mensajería y el streaming de eventos entre los procesos de ingestión, transformación y visualización. Su elección se basa en su escalabilidad, robustez y facilidad de integración con múltiples consumidores simultáneos, como dashboards o alertas.

- **Airflow**: Orquestador de pipelines ETL/ELT. Permite programar y disparar DAGs completos, incluyendo ingestión de datos históricos, transformaciones dbt y generación de alertas. Se eligió por su capacidad de monitoreo, reintentos automáticos y escalabilidad para pipelines complejos.

- **Python**: Lenguaje principal para la implementación de scripts de ingestión, transformaciones intermedias y generación de alertas. Se eligió por su amplia adopción y facilidad de integración con Flask, Kafka y dbt.

- **Plotly**: Framework para dashboards interactivos. Permite crear visualizaciones dinámicas y diferenciadas para **población general** y **población de riesgo**. Se eligió por su capacidad de integrar gráficos avanzados directamente con Python.

- **Docker**: Todos los servicios se despliegan en contenedores individuales con Docker. Cada servicio tiene su **Dockerfile específico** para reproducibilidad y escalabilidad. Se eligió Docker para:
  - Automatizar el despliegue de múltiples servicios.
  - Facilitar la escalabilidad.
  - Garantizar entornos consistentes en desarrollo y producción.

---

## Contribuidores
- Javier Plaza
- Jorge Albalat
- Salvador Reche
- Marina Azul López

