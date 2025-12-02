import yaml
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from etl_core import ETLEngine
from sqlalchemy import text
import logging

app = Flask(__name__)
# Habilitar CORS para permitir peticiones desde tu React (localhost:8080)
CORS(app) 

# Configurar logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/api/run', methods=['POST'])
def run_etl():
    """Ejecuta el pipeline de migración bajo demanda"""
    try:
        logger.info(" Recibida petición de ejecución ETL...")
        engine = ETLEngine()
        
        # Ejecutamos la lógica que ya probaste
        engine.run_pipeline()
        
        return jsonify({
            "status": "success", 
            "message": "Pipeline ejecutado correctamente. Datos migrados a QA."
        }), 200
    except Exception as e:
        logger.error(f"Error en API: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    try:
        etl = ETLEngine()
        with etl.engine_qa.connect() as conn:
            # Consultamos los campos nuevos
            result = conn.execute(text("""
                SELECT fecha_fin, tabla, registros_procesados, estado, mensaje, operacion, id_ejecucion 
                FROM auditoria 
                ORDER BY fecha_fin DESC 
                LIMIT 50
            """))
            
            logs = []
            for row in result:
                logs.append({
                    "fecha": str(row[0]),
                    "tabla": row[1],
                    "registros": row[2],
                    "estado": row[3],
                    "mensaje": f"[{row[5]}] {row[4]} (ID: {row[6]})" # Combinamos info extra en el mensaje
                })
                
        return jsonify(logs), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "online", "system": "DataMask ETL"}), 200

@app.route('/api/pipelines', methods=['POST'])
def create_pipeline():
    """Agrega una nueva tabla al archivo config.yaml"""
    try:
        data = request.json
        nombre_job = data.get('name')
        tabla_origen = data.get('table')
        
        if not nombre_job or not tabla_origen:
            return jsonify({"status": "error", "message": "Faltan datos"}), 400

        # 1. Leer el config actual
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # 2. Verificar si ya existe
        for tabla in config['tables']:
            if tabla['name'] == tabla_origen:
                return jsonify({"status": "error", "message": f"La tabla {tabla_origen} ya está configurada"}), 409

        # 3. Crear la nueva configuración (con defaults inteligentes)
        nuevo_job = {
            "name": tabla_origen, # Usamos el nombre de la tabla como ID interno
            "pk": "id",
            "filter_column": "fecha_registro",
            "masking_rules": {
                # Reglas por defecto para seguridad
                "email": "hash_email", 
                "telefono": "preserve_format"
            }
        }
        
        # 4. Agregar y Guardar
        config['tables'].append(nuevo_job)
        
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
            
        logger.info(f"✅ Nuevo pipeline guardado en YAML: {nombre_job}")
        
        return jsonify({
            "status": "success", 
            "message": f"Pipeline '{nombre_job}' configurado y guardado en disco."
        }), 201

    except Exception as e:
        logger.error(f"Error guardando pipeline: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # Ejecutar en puerto 5000
    print(" Servidor API escuchando en http://localhost:5000")
    app.run(debug=True, port=5000)