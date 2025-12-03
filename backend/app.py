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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

def load_config():
    path = os.path.join(BASE_DIR, 'config.yaml')
    with open(path, 'r') as f: return yaml.safe_load(f)

def save_config(config):
    path = os.path.join(BASE_DIR, 'config.yaml')
    with open(path, 'w') as f: yaml.dump(config, f, sort_keys=False)

# --- 1. GESTIÓN DE REGLAS (ENMASCARAMIENTO) ---
@app.route('/api/rules', methods=['GET', 'POST', 'DELETE'])
def handle_rules():
    config_path = os.path.join(BASE_DIR, 'config.yaml')
    
    if request.method == 'GET':
        try:
            with open(config_path, 'r') as f: config = yaml.safe_load(f)
            rules_list = []
            for t in config.get('tables', []):
                table_name = t.get('name')
                for col, rule_type in t.get('masking_rules', {}).items():
                    rules_list.append({
                        "id": f"{table_name}-{col}",
                        "name": f"{col} ({rule_type})",
                        "table": table_name,
                        "column": col,
                        "type": rule_type,
                        "isActive": True
                    })
            return jsonify(rules_list)
        except Exception as e: return jsonify([]), 500

    if request.method == 'POST':
        try:
            data = request.json
            table_target = data.get('table')
            col_target = data.get('column')
            rule_type = data.get('type')

            with open(config_path, 'r') as f: config = yaml.safe_load(f)
            
            found = False
            for t in config['tables']:
                if t['name'] == table_target:
                    if 'masking_rules' not in t: t['masking_rules'] = {}
                    t['masking_rules'][col_target] = rule_type
                    found = True
                    break
            
            if not found: return jsonify({"error": "Tabla no encontrada"}), 404

            save_config(config)
            return jsonify({"status": "success", "message": "Regla aplicada"}), 200
        except Exception as e: return jsonify({"error": str(e)}), 500

    if request.method == 'DELETE':
        try:
            data = request.json
            table_target = data.get('table')
            col_target = data.get('column')

            with open(config_path, 'r') as f: config = yaml.safe_load(f)
            
            for t in config['tables']:
                if t['name'] == table_target and 'masking_rules' in t:
                    if col_target in t['masking_rules']:
                        del t['masking_rules'][col_target]
            
            save_config(config)
            return jsonify({"status": "success", "message": "Regla eliminada"}), 200
        except Exception as e: return jsonify({"error": str(e)}), 500

# --- 2. INSPECTOR DE COLUMNAS (FALTABA ESTO) ---
@app.route('/api/source/columns/<table_name>', methods=['GET'])
def get_columns(table_name):
    try:
        config = load_config()
        prod_uri = os.getenv(config['databases']['source_db_env_var'])
        inspector = inspect(create_engine(prod_uri))
        columns = [c['name'] for c in inspector.get_columns(table_name)]
        return jsonify(columns)
    except Exception as e: return jsonify({"error": str(e)}), 500

# --- 3. PIPELINES (GET + POST) ---
@app.route('/api/pipelines', methods=['GET', 'POST'])
def handle_pipelines():
    config = load_config()
    
    if request.method == 'GET':
        try:
            # Devolver lista de tablas configuradas
            return jsonify([{
                "id": t['name'], 
                "name": t.get('description', f"Migración {t['name']}"), # Nombre amigable 
                "sourceDb": "supabase-prod", 
                "targetDb": "supabase-qa",
                "status": "idle", 
                "tablesCount": 1, 
                "maskingRulesCount": len(t.get('masking_rules', {}))
            } for t in config['tables']])
        except Exception: return jsonify([])

    if request.method == 'POST':
        # Crear nuevo pipeline
        try:
            data = request.json
            tabla = data.get('table')
            
            if any(t['name'] == tabla for t in config['tables']):
                return jsonify({"status": "error", "message": "Ya existe"}), 409

            # Auto-detección básica
            prod_uri = os.getenv(config['databases']['source_db_env_var'])
            inspector = inspect(create_engine(prod_uri))
            columns = inspector.get_columns(tabla)
            
            masking_rules = {}
            filter_col = None
            
            for col in columns:
                cname = col['name'].lower()
                if not filter_col and ('fecha' in cname or 'date' in cname): filter_col = col['name']
                if 'email' in cname: masking_rules[col['name']] = 'hash_email'
                elif 'telef' in cname: masking_rules[col['name']] = 'preserve_format'
                elif 'nombre' in cname: masking_rules[col['name']] = 'fake_name'

            new_job = {
                "name": tabla,
                "description": data.get('name'), # Guardamos nombre custom
                "pk": "id",
                "filter_column": filter_col,
                "masking_rules": masking_rules
            }
            
            config['tables'].append(new_job)
            save_config(config)
            return jsonify({"status": "success"}), 201
        except Exception as e: return jsonify({"error": str(e)}), 500

# --- OTROS ENDPOINTS (DASHBOARD, RUN, ETC) ---
@app.route('/api/source/tables', methods=['GET'])
def get_source_tables():
    try:
        config = load_config()
        prod_uri = os.getenv(config['databases']['source_db_env_var'])
        return jsonify(inspect(create_engine(prod_uri)).get_table_names())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard():
    try:
        config = load_config()
        etl = ETLEngine()
        with etl.engine_qa.connect() as conn:
            total = conn.execute(text("SELECT SUM(registros_procesados) FROM auditoria")).scalar() or 0
        return jsonify({
            "kpi": { "pipelines": len(config['tables']), "rules": 0, "records": total, "success_rate": 100 },
            "chart_data": [], "recent_activity": [], "system_status": {}
        }) # Simplificado para evitar errores si BD está vacía
    except: return jsonify({})

@app.route('/api/run', methods=['POST'])
def run_etl():
    try:
        data = request.json or {}
        ETLEngine().run_pipeline(target_table=data.get('table'))
        return jsonify({"status": "success", "message": "Ejecutado"}), 200
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    try:
        with ETLEngine().engine_qa.connect() as conn:
            res = conn.execute(text("SELECT fecha_ejecucion, tabla, registros_procesados, estado, mensaje FROM auditoria ORDER BY fecha_ejecucion DESC LIMIT 50"))
            return jsonify([{"fecha": str(r[0]), "tabla": r[1], "registros": r[2], "estado": r[3], "mensaje": r[4]} for r in res])
    except: return jsonify([])

@app.route('/api/connections', methods=['GET'])
def get_connections():
    return jsonify([{"id": "prod", "name": "Producción", "status": "connected", "isProduction": True}, {"id": "qa", "name": "QA", "status": "connected", "isProduction": False}])

@app.route('/api/settings', methods=['GET'])
def get_settings(): return jsonify(load_config().get('settings', {}))

@app.route('/api/rules/reset', methods=['POST'])
def reset_rules():
    """Restaura las 9 reglas por defecto en config.yaml"""
    try:
        config = load_config()
        
        # Definición de la "Plantilla Maestra" (Los 9 Defaults)
        defaults = {
            "clientes": {
                "nombre": "fake_name",
                "email": "hash_email",
                "telefono": "preserve_format",
                "direccion": "redact"
            },
            "ordenes": {
                "total": "none"
            },
            "detalle_ordenes": {
                "producto": "hash_email",
                "precio_unitario": "none"
            },
            "inventario": {
                "producto": "hash_email",
                "ubicacion": "redact"
            }
        }

        # Aplicar la plantilla a las tablas existentes
        updated_count = 0
        for t in config['tables']:
            t_name = t['name']
            if t_name in defaults:
                # Sobrescribimos las reglas de esa tabla con los defaults
                t['masking_rules'] = defaults[t_name]
                updated_count += len(defaults[t_name])
        
        save_config(config)
        logger.info("♻️ Reglas restauradas a valores de fábrica.")
        
        return jsonify({
            "status": "success", 
            "message": f"Se han restaurado {updated_count} reglas por defecto."
        }), 200

    except Exception as e:
        logger.error(f"Error reset reglas: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    print("🚀 API Backend lista en http://localhost:5000")
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: ETLEngine().run_pipeline(), 'interval', minutes=5)
    scheduler.start()
    app.run(debug=True, port=5000, use_reloader=False)