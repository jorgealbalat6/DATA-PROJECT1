from pyspark.sql import SparkSession
import os
import psycopg
import time

for i in range(10):
    try:
        url = os.getenv("DATABASE_URL")
        connection = psycopg.connect(url)
        print("BD conectada con éxito")
        connection.close()
        break
    except Exception as e:
        print(f"Esperando a la BD... ({e})")
        time.sleep(2)

jdbc_url = os.getenv("JDBC_URL")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
database_url = os.getenv("DATABASE_URL") 

spark = SparkSession.builder \
    .appName("AnalisisHistorico") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", 'public."marts_CalidadAire"') \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
df.show(5)
