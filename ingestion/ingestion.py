import os, psycopg, requests, time

print('start')

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


data = requests.get('https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000')
data = data.json().get('records')

def InsertarDatos(datos):
    #print(datos)
    lista_inserciones = []

    # Iteramos desde la hora 1 hasta la 24
    for hora in range(1, 25):
        # Creamos las claves dinámicamente: 
        # Si hora es 1, genera "H01" y "V01". Si es 10, genera "H10" y "V10".
        clave_valor = f"H{hora:02d}"      # f-string con padding de ceros
        clave_validacion = f"V{hora:02d}"

        # Extraemos los valores comunes
        municipio = int(datos['MUNICIPIO'])
        estacion = int(datos['ESTACION'])
        magnitud = int(datos['MAGNITUD'])
        punto = datos['PUNTO_MUESTREO']
        ano = int(datos['ANO'])
        mes = int(datos['MES'])
        dia = int(datos['DIA'])

        # Extraemos los valores específicos de esa hora
        valor_hora = float(datos[clave_valor])
        validacion_hora = datos[clave_validacion]

        # Creamos la tupla para esta hora específica
        fila = (
            municipio, 
            estacion, 
            magnitud, 
            punto, 
            ano, 
            mes, 
            dia, 
            hora,           # Aquí va el número de hora (1-24)
            valor_hora,     # El valor de contaminación
            validacion_hora # El código de validación (V/N)
        )
        
        lista_inserciones.append(fila)
    for i in lista_inserciones:
        cur.execute("""INSERT INTO calidad_aire_madrid (MUNICIPIO, ESTACION, MAGNITUD, PUNTO_MUESTREO, ANO, MES, DIA, HORA, VALOR, VALIDACION) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9]))
    connection.commit()


for datos in data:
    InsertarDatos(datos)
print("Datos insertados correctamente")
