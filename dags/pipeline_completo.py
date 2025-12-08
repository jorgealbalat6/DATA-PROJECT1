import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# Configuración básica
default_args = {
    'owner': 'EDEM_MDA_DATA_PROJECT1',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    'flujo_etl_completo',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    start = EmptyOperator(task_id='inicio_flujo')

    # Variables de entorno comunes para todos los contenedores
    env_vars = {
        'POSTGRES_USER': os.getenv('POSTGRES_USER'),
        'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
        'POSTGRES_DB': os.getenv('POSTGRES_DB', 'postgres'),
        'POSTGRES_HOST': 'db', 
        'DB_HOST': 'db', 
        'DB_PORT': '5432',
        'POSTGRES_PORT': '5432',
        'POSTGRES_SCHEMA': 'public',
        'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
        'API_URL': 'http://api:5000'
    }

    t_ingest_general = DockerOperator(
        task_id='ingestion_general',
        image='ingestion:latest', 
        api_version='auto',
        auto_remove=True,
        network_mode='kafka-net',
        docker_url="unix://var/run/docker.sock",
        environment=env_vars,
        mount_tmp_dir=False,
        force_pull=False
    )


    t_dbt = DockerOperator(
        task_id='dbt_transformacion',
        image='dbt:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='kafka-net',
        docker_url="unix://var/run/docker.sock",
        environment=env_vars,
        mount_tmp_dir=False,
        force_pull=False
    )

    # Definición de dependencias                
    start >> t_ingest_general >> t_dbt