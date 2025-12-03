import yaml
import os
import logging
import atexit
import time  # <--- FALTABA ESTO
from datetime import datetime # <--- FALTABA ESTO
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

# --- 1. ENDPOINT CONEXIONES (Con Latencia) ---
@app.route('/api/connections', methods=['GET'])
def get_connections():
    config = load_config()
    connections = []
    
    dbs = [
        {"id": "prod", "name": "Producción (Origen)", "var": config['databases']['source_db_env_var'], "isProd": True},
        {"id": "qa", "name": "QA (Destino)", "var": config['databases']['target_db_env_var'], "isProd": False}
    ]
    
    for db in dbs:
        uri = os.getenv(db['var'])
        status = "disconnected"
        host = "desconocido"
        latency = 0
        version = "Unknown"
        
        if uri:
            try:
                if '@' in uri: host = uri.split('@')[1].split(':')[0]
                
                # Medir Latencia
                start_time = time.time()
                engine = create_engine(uri)
                with engine.connect() as conn:
                    ver = conn.execute(text("SHOW server_version")).scalar()
                    version = f"PostgreSQL {ver}"
                end_time = time.time()
                
                latency = round((end_time - start_time) * 1000) # ms
                status = "connected"
            except Exception as e:
                status = "error"
                logger.error(f"Error conexión {db['name']}: {e}")
        
        connections.append({
            "id": db['id'],
            "name": db['name'],
            "type": "postgresql",
            "host": host,
            "status": status,
            "isProduction": db['isProd'],
            "latency": latency,
            "version": version,
            "lastChecked": datetime.now().isoformat() # <--- Aquí daba el error
        })
        
    return jsonify(connections)

# --- 2. GESTIÓN DE REGLAS ---
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
        except Exception: return jsonify([])

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
            return jsonify({"status": "success"})
        except Exception as e: return jsonify({"error": str(e)}), 500

    if request.method == 'DELETE':
        try:
            data = request.json
            table_target = data.get('table')
            col_target = data.get('column')
            with open(config_path, 'r') as f: config = yaml.safe_load(f)
            for t in config['tables']:
                if t['name'] == table_target and 'masking_rules' in t:
                    if col_target in t['masking_rules']: del t['masking_rules'][col_target]
            save_config(config)
            return jsonify({"status": "success"})
        except Exception as e: return jsonify({"error": str(e)}), 500

# --- 3. RUTA RESET DEFAULTS ---
@app.route('/api/rules/reset', methods=['POST'])
def reset_rules():
    try:
        config = load_config()
        defaults = {
            "clientes": {"nombre": "fake_name", "email": "hash_email", "telefono": "preserve_format", "direccion": "redact"},
            "ordenes": {"total": "none"},
            "detalle_ordenes": {"producto": "hash_email", "precio_unitario": "none"},
            "inventario": {"producto": "hash_email", "ubicacion": "redact"}
        }
        for t in config['tables']:
            if t['name'] in defaults: t['masking_rules'] = defaults[t['name']]
        save_config(config)
        return jsonify({"status": "success", "message": "Reglas restauradas"}), 200
    except Exception as e: return jsonify({"error": str(e)}), 500

# --- 4. PIPELINES & DASHBOARD ---
@app.route('/api/pipelines', methods=['GET', 'POST'])
def handle_pipelines():
    config = load_config()
    if request.method == 'GET':
        try:
            pipelines_list = []
            engine = ETLEngine().engine_qa
            with engine.connect() as conn:
                for t in config.get('tables', []):
                    table_name = t.get('name')
                    friendly_name = t.get('description', f"Migración {table_name.capitalize()}")
                    last_run = conn.execute(text(f"SELECT fecha_ejecucion, estado, registros_procesados FROM auditoria WHERE tabla = '{table_name}' ORDER BY fecha_ejecucion DESC LIMIT 1")).fetchone()
                    status, last_date, recs = "idle", None, 0
                    if last_run:
                        status = "success" if "SUCCESS" in last_run[1] else "error"
                        last_date, recs = str(last_run[0]), last_run[2]
                    pipelines_list.append({
                        "id": table_name, "name": friendly_name, 
                        "sourceDb": "supabase-prod", "targetDb": "supabase-qa",
                        "status": status, "lastRun": last_date,
                        "tablesCount": 1, "maskingRulesCount": len(t.get('masking_rules', {})),
                        "recordsProcessed": recs
                    })
            return jsonify(pipelines_list)
        except: return jsonify([])

    if request.method == 'POST':
        data = request.json
        if any(t['name'] == data['table'] for t in config['tables']): return jsonify({"status": "error", "message": "Ya existe"}), 409
        
        # Auto-config básico
        try:
            prod_uri = os.getenv(config['databases']['source_db_env_var'])
            inspector = inspect(create_engine(prod_uri))
            columns = inspector.get_columns(data['table'])
            masking = {}
            filter_col = None
            for col in columns:
                cname = col['name'].lower()
                if not filter_col and ('fecha' in cname or 'date' in cname): filter_col = col['name']
                if 'email' in cname: masking[col['name']] = 'hash_email'
                elif 'nomb' in cname: masking[col['name']] = 'fake_name'
            
            config['tables'].append({
                "name": data['table'], "description": data.get('name'), 
                "pk": "id", "filter_column": filter_col, "masking_rules": masking
            })
            save_config(config)
            return jsonify({"status": "success"}), 201
        except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard():
    try:
        config = load_config()
        etl = ETLEngine()
        total_rules = sum(len(t.get('masking_rules', {})) for t in config.get('tables', []))
        
        with etl.engine_qa.connect() as conn:
            res_total = conn.execute(text("SELECT SUM(registros_procesados) FROM auditoria")).scalar() or 0
            res_ok = conn.execute(text("SELECT COUNT(*) FROM auditoria WHERE estado LIKE 'SUCCESS%'")).scalar() or 0
            res_count = conn.execute(text("SELECT COUNT(*) FROM auditoria")).scalar() or 1
            success_rate = int((res_ok / res_count) * 100)
            chart_data = [{"name": r[0], "value": r[1]} for r in conn.execute(text("SELECT tabla, SUM(registros_procesados) as total FROM auditoria GROUP BY tabla ORDER BY total DESC LIMIT 5"))]
            recent = [{"table": r[0], "status": "success" if "SUCCESS" in r[1] else "error", "time": str(r[2]), "records": r[3]} for r in conn.execute(text("SELECT tabla, estado, fecha_ejecucion, registros_procesados FROM auditoria ORDER BY fecha_ejecucion DESC LIMIT 5"))]

        return jsonify({
            "kpi": { "pipelines": len(config['tables']), "rules": total_rules, "records": res_total, "success_rate": success_rate },
            "chart_data": chart_data, "recent_activity": recent,
            "system_status": { "api": "online", "scheduler": "running", "db_prod": "connected", "db_qa": "connected" }
        })
    except: return jsonify({})

# --- OTROS ---
@app.route('/api/run', methods=['POST'])
def run_etl():
    data = request.json or {}
    try:
        ETLEngine().run_pipeline(target_table=data.get('table'))
        return jsonify({"status": "success", "message": "Ejecutado"}), 200
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    try:
        with ETLEngine().engine_qa.connect() as conn:
            res = conn.execute(text("SELECT fecha_ejecucion, tabla, registros_procesados, estado, mensaje FROM auditoria ORDER BY fecha_ejecucion DESC LIMIT 50"))
            return jsonify([{"fecha": str(r[0]), "tabla": r[1], "registros": r[2], "estado": r[3], "mensaje": r[4]} for r in res])
    except: return jsonify([])

@app.route('/api/source/tables', methods=['GET'])
def get_source_tables():
    try:
        config = load_config()
        prod_uri = os.getenv(config['databases']['source_db_env_var'])
        return jsonify(inspect(create_engine(prod_uri)).get_table_names())
    except: return jsonify([])

@app.route('/api/source/columns/<table_name>', methods=['GET'])
def get_columns(table_name):
    try:
        config = load_config()
        prod_uri = os.getenv(config['databases']['source_db_env_var'])
        return jsonify([c['name'] for c in inspect(create_engine(prod_uri)).get_columns(table_name)])
    except: return jsonify([])

@app.route('/api/settings', methods=['GET', 'POST'])
def handle_settings():
    if request.method == 'GET': return jsonify(load_config().get('settings', {}))
    if request.method == 'POST':
        config = load_config()
        for k,v in request.json.items(): 
            if k in config['settings']: config['settings'][k] = v
            elif k in ['notifications','security','scheduler']: config['settings'].setdefault(k, {}).update(v)
        save_config(config)
        return jsonify({"status": "success"})

if __name__ == '__main__':
    print("🚀 Servidor Listo (con datetime)")
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: ETLEngine().run_pipeline(), 'interval', minutes=5)
    scheduler.start()
    app.run(debug=True, port=5000, use_reloader=False)