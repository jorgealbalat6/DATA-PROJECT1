from flask import Flask, request, jsonify
import psycopg
import os
import io

app = Flask(__name__)

# Función para obtener conexión a BD
def get_db_connection():
    url = os.getenv("DATABASE_URL")
    return psycopg.connect(url)

@app.route('/ingest/calidad_aire', methods=['POST'])
def ingest_data():
    # 1. Verificar si envían un archivo
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    try:
        # 2. Leer el archivo en memoria (stream)
        # Convertimos los bytes del archivo a un string para que COPY lo entienda
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        
        # 3. Conectar a la BD y hacer el COPY
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                COPY calidad_aire (municipio_id, estacion_id, magnitud_id, punto_muestreo, ANO, MES, DIA,
                H01, V01, H02, V02, H03, V03, H04, V04, H05, V05, H06, V06, H07, V07, H08, V08, H09, V09, H10, V10,
                H11, V11, H12, V12, H13, V13, H14, V14, H15, V15, H16, V16, H17, V17, H18, V18, H19, V19,
                H20, V20, H21, V21, H22, V22, H23, V23, H24, V24)
                FROM STDIN
                WITH (FORMAT CSV)
                """
                with cur.copy(sql) as copy:
                    copy.write(stream.getvalue())
            conn.commit()

        return jsonify({'message': 'Datos ingestados correctamente en PostgreSQL'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)