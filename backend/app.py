import yaml
import os
import logging
import atexit
from flask import Flask, jsonify, request
from flask_cors import CORS
from etl_core import ETLEngine
from sqlalchemy import text, create_engine, inspect
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

# Configuración
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

def load_config():
    path = os.path.join(BASE_DIR, 'config.yaml')
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def save_config(config):
    path = os.path.join(BASE_DIR, 'config.yaml')
    with open(path, 'w') as f:
        yaml.dump(config, f, sort_keys=False)

# --- ENDPOINT: DESCUBRIMIENTO DE TABLAS (SIN FILTROS) ---
@app.route('/api/source/tables', methods=['GET'])
def get_source_tables():
    try:
        config = load_config()
        prod_uri = os.getenv(config['databases']['source_db_env_var'])
        
        # 1. Conectar a Producción
        engine = create_engine(prod_uri)
        inspector = inspect(engine)
        
        # 2. Obtener TODAS las tablas
        all_tables = inspector.get_table_names()
        
        # 3. (Opcional) Filtrar solo tablas internas de sistema si estorban
        visible_tables = [t for t in all_tables if t not in ['alembic_version', 'spatial_ref_sys']]
        
        logger.info(f"Tablas encontradas: {visible_tables}")
        return jsonify(visible_tables)
    except Exception as e:
        logger.error(f"Error buscando tablas: {e}")
        return jsonify({"error": str(e)}), 500

# --- ENDPOINT: DASHBOARD ---
@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_stats():
    try:
        config = load_config()
        etl = ETLEngine()
        total_pipelines = len(config.get('tables', []))
        total_rules = sum(len(t.get('masking_rules', {})) for t in config.get('tables', []))
        
        with etl.engine_qa.connect() as conn:
            res_total = conn.execute(text("SELECT SUM(registros_procesados) FROM auditoria")).scalar() or 0
            res_ok = conn.execute(text("SELECT COUNT(*) FROM auditoria WHERE estado LIKE 'SUCCESS%'")).scalar() or 0
            res_count = conn.execute(text("SELECT COUNT(*) FROM auditoria")).scalar() or 1
            success_rate = int((res_ok / res_count) * 100)

        return jsonify({
            "pipelines": total_pipelines,
            "rules": total_rules,
            "records": res_total,
            "success_rate": success_rate
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- ENDPOINT: CREAR PIPELINE INTELIGENTE ---
@app.route('/api/pipelines', methods=['GET', 'POST'])
def handle_pipelines():
    config_path = os.path.join(BASE_DIR, 'config.yaml')
    
    if request.method == 'GET':
        try:
            with open(config_path, 'r') as f: config = yaml.safe_load(f)
            pipelines_list = []
            engine = ETLEngine().engine_qa
            
            with engine.connect() as conn:
                for t in config.get('tables', []):
                    table_name = t.get('name')
                    # Busca el estado real
                    last_run = conn.execute(text(f"SELECT fecha_ejecucion, estado, registros_procesados FROM auditoria WHERE tabla = '{table_name}' ORDER BY fecha_ejecucion DESC LIMIT 1")).fetchone()
                    
                    status, last_date, recs = "idle", None, 0
                    if last_run:
                        status = "success" if "SUCCESS" in last_run[1] else "error"
                        last_date, recs = str(last_run[0]), last_run[2]

                    pipelines_list.append({
                        "id": table_name,
                        "name": f"Migración {table_name}",
                        "description": f"Job automático para tabla '{table_name}'",
                        "sourceDb": "supabase-prod",
                        "targetDb": "supabase-qa",
                        "status": status,
                        "lastRun": last_date,
                        "tablesCount": 1,
                        "maskingRulesCount": len(t.get('masking_rules', {})),
                        "recordsProcessed": recs
                    })
            return jsonify(pipelines_list)
        except Exception as e: return jsonify([]), 500

    if request.method == 'POST':
        try:
            data = request.json
            tabla = data.get('table')
            nombre = data.get('name')
            
            if not tabla: return jsonify({"status": "error", "message": "Falta tabla"}), 400

            config = load_config()
            # Verificar duplicados
            for t in config['tables']:
                if t['name'] == tabla:
                    return jsonify({"status": "error", "message": "Tabla ya configurada"}), 409

            # Inspección Inteligente
            prod_uri = os.getenv(config['databases']['source_db_env_var'])
            inspector = inspect(create_engine(prod_uri))
            columns = inspector.get_columns(tabla)
            
            masking_rules = {}
            filter_col = None
            
            for col in columns:
                cname = col['name'].lower()
                if not filter_col and ('fecha' in cname or 'date' in cname): filter_col = col['name']
                
                # Reglas Automáticas
                if 'email' in cname: masking_rules[col['name']] = 'hash_email'
                elif 'telefono' in cname: masking_rules[col['name']] = 'preserve_format'
                elif 'nombre' in cname: masking_rules[col['name']] = 'fake_name'
                elif 'direc' in cname or 'producto' in cname: masking_rules[col['name']] = 'redact'

            new_job = {
                "name": tabla,
                "pk": "id",
                "filter_column": filter_col,
                "masking_rules": masking_rules
            }
            
            config['tables'].append(new_job)
            save_config(config)
            return jsonify({"status": "success", "message": f"Pipeline auto-configurado para {tabla}"}), 201

        except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

# --- OTROS ENDPOINTS ---
@app.route('/api/run', methods=['POST'])
def run_etl():
    try:
        logger.info("📡 Ejecución manual")
        engine = ETLEngine()
        engine.run_pipeline()
        return jsonify({"status": "success", "message": "Pipeline ejecutado."}), 200
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    try:
        etl = ETLEngine()
        with etl.engine_qa.connect() as conn:
            # Ajustado a la estructura final de tu tabla auditoria
            result = conn.execute(text("SELECT fecha_ejecucion, tabla, registros_procesados, estado, mensaje FROM auditoria ORDER BY fecha_ejecucion DESC LIMIT 50"))
            logs = [{"fecha": str(r[0]), "tabla": r[1], "registros": r[2], "estado": r[3], "mensaje": r[4]} for r in result]
        return jsonify(logs), 200
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/connections', methods=['GET'])
def get_connections():
    config = load_config()
    return jsonify([
        {"id": "prod", "name": "Producción", "status": "connected", "isProduction": True, "host": "supabase-cloud"},
        {"id": "qa", "name": "QA / Auditoría", "status": "connected", "isProduction": False, "host": "supabase-cloud"}
    ])

@app.route('/api/settings', methods=['GET'])
def get_settings():
    return jsonify(load_config().get('settings', {}))

if __name__ == '__main__':
    print("🚀 Servidor API listo en http://localhost:5000")
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: ETLEngine().run_pipeline(), 'interval', minutes=5)
    scheduler.start()
    app.run(debug=True, port=5000, use_reloader=False)