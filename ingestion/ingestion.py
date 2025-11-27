import os, psycopg, requests, time

print('start')
try:
    #URL CONEXIÓN A BD 
    url = os.getenv("DATABASE_URL")
    #CONEXIÓN A BD
    connection = psycopg.connect(url)
    # Cursor
    cur = connection.cursor()
    print("BD conectada con éxito")
except:
    print("Error conectando a la BD")


data = requests.get('https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000')
data = data.json().get('records')

def InsertarDatos(datos):
    #print(datos)
    cur.execute("""INSERT INTO calidad_aire (punto_muestreo, municipio_id, estacion_id, magnitud_id, ANO, MES, DIA,
                        H01, V01, H02, V02, H03, V03, H04, V04, H05, V05, H06, V06, H07, V07, H08, V08, H09, V09, H10, V10,
                        H11, V11, H12, V12, H13, V13, H14, V14, H15, V15, H16, V16, H17, V17, H18, V18, H19, V19,
                        H20, V20, H21, V21, H22, V22, H23, V23, H24, V24) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (datos['PUNTO_MUESTREO'], float(datos['MUNICIPIO']), float(datos['ESTACION']), float(datos['MAGNITUD']), float(datos['ANO']), float(datos['MES']), float(datos['DIA']),
                float(datos['H01']), datos['V01'], float(datos['H02']), datos['V02'], float(datos['H03']), datos['V03'], float(datos['H04']), datos['V04'], float(datos['H05']), datos['V05'], float(datos['H06']), datos['V06'],
                float(datos['H07']), datos['V07'], float(datos['H08']), datos['V08'], float(datos['H09']), datos['V09'], float(datos['H10']), datos['V10'], float(datos['H11']), datos['V11'], float(datos['H12']), datos['V12'],
                float(datos['H13']), datos['V13'], float(datos['H14']), datos['V14'], float(datos['H15']), datos['V15'], float(datos['H16']), datos['V16'], float(datos['H17']), datos['V17'], float(datos['H18']), datos['V18'],
                float(datos['H19']), datos['V19'], float(datos['H20']), datos['V20'], float(datos['H21']), datos['V21'], float(datos['H22']), datos['V22'], float(datos['H23']), datos['V23'], float(datos['H24']), datos['V24']))
    connection.commit()


for i in data:
    InsertarDatos(i)
print("Datos insertados correctamente")
