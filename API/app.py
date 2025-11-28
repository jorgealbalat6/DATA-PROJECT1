from flask import Flask, request, jsonify
import psycopg
import os
import io

app = Flask(__name__)

def get_db_connection():
    url = os.getenv("DATABASE_URL")
    return psycopg.connect(url)

@app.route('/ingest/calidad_aire', methods=['POST'])
def ingest_data():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    try:
        # Decodificamos
        stream = io.StringIO(file.stream.read().decode("utf-8-sig"), newline=None)
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # --- CAMBIO: Usamos los nombres que TU tabla ya tiene (sin _id) ---
                sql = """
                COPY calidad_aire_madrid (
                    MUNICIPIO, 
                    ESTACION, 
                    MAGNITUD, 
                    PUNTO_MUESTREO, 
                    ANO, 
                    MES, 
                    DIA, 
                    HORA, 
                    VALOR, 
                    VALIDACION
                )
                FROM STDIN
                WITH (FORMAT CSV)
                """
                with cur.copy(sql) as copy:
                    copy.write(stream.getvalue())
            conn.commit()

        return jsonify({'message': 'Datos ingestados correctamente'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)