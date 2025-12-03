import yaml
import os
import logging
import atexit
from flask import Flask, jsonify, request
from flask_cors import CORS
from etl_core import ETLEngine
from sqlalchemy import text, create_engine, inspect  # <--- Importante: inspect
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# --- UTILIDADES ---
def load_config():
    path = os.path.join(BASE_DIR, 'config.yaml')
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def save_config(config):
    path = os.path.join(BASE_DIR, 'config.yaml')
    with open(path, 'w') as f:
        yaml.dump(config, f, sort_keys=False)

# --- ENDPOINTS DE GESTIÓN ---

@app.route('/api/source/tables', methods=['GET'])
def get_available_tables():
    """Devuelve las tablas de Producción que AÚN NO están configuradas"""
    try:
        config = load_config()
        prod_uri = os.getenv(config['databases']['source_db_env_var'])
        
        # 1. Conectar y obtener todas las tablas reales
        engine = create_engine(prod_uri)
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()
        
        # 2. Filtrar las que ya tenemos configuradas
        configured_tables = [t['name'] for t in config['tables']]
        new_tables = [t for t in all_tables if t not in configured_tables]
        
        return jsonify(new_tables)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_stats():
    """Obtiene métricas reales para el Home"""
    try:
        config = load_config()
        etl = ETLEngine()
        
        # 1. Contar Pipelines y Reglas
        total_pipelines = len(config.get('tables', []))
        total_rules = sum(len(t.get('masking_rules', {})) for t in config.get('tables', []))
        
        # 2. Consultar BD de Auditoría para métricas de ejecución
        with etl.engine_qa.connect() as conn:
            # Total registros procesados (histórico)
            res_total = conn.execute(text("SELECT SUM(registros_procesados) FROM auditoria")).scalar() or 0
            
            # Tasa de éxito (últimas 100 ejecuciones)
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

@app.route('/api/connections', methods=['GET'])
def get_connections():
    """Prueba y devuelve el estado de las conexiones definidas en .env"""
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
        
        if uri:
            try:
                # Extraer host visualmente (ocultando pass)
                host = uri.split('@')[1].split(':')[0] if '@' in uri else "localhost"
                # Probar conexión real
                engine = create_engine(uri)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                status = "connected"
            except Exception:
                status = "error"
        
        connections.append({
            "id": db['id'],
            "name": db['name'],
            "type": "postgresql", # Asumimos PG por Supabase
            "host": host,
            "status": status,
            "isProduction": db['isProd']
        })
        
    return jsonify(connections)

@app.route('/api/settings', methods=['GET', 'POST'])
def handle_settings():
    """Lee o Modifica la configuración global (Batch size, días)"""
    if request.method == 'GET':
        config = load_config()
        return jsonify(config.get('settings', {}))
    
    if request.method == 'POST':
        new_settings = request.json
        config = load_config()
        # Actualizar solo lo que llega
        for k, v in new_settings.items():
            config['settings'][k] = v
        save_config(config)
        return jsonify({"status": "success", "message": "Configuración actualizada"})

@app.route('/api/rules', methods=['GET', 'POST'])
def handle_rules():
    """Gestión de reglas de enmascaramiento"""
    config = load_config()
    
    if request.method == 'GET':
        # Transformar formato YAML a lista plana para el Frontend
        rules_list = []
        for idx, table in enumerate(config['tables']):
            for col, rule_type in table.get('masking_rules', {}).items():
                rules_list.append({
                    "id": f"{table['name']}-{col}",
                    "name": f"Regla: {col}",
                    "type": rule_type, # hash_email, redact, etc.
                    "replacement": "(Dinámico)", 
                    "tables": [table['name']],
                    "columns": [col],
                    "isActive": True # En yaml si está escrita, está activa
                })
        return jsonify(rules_list)

    # POST: Agregar una nueva regla (Simplificado para Demo)
    if request.method == 'POST':
        data = request.json
        # Lógica para inyectar regla en el YAML...
        # (Para la demo, basta con que el GET funcione bien para mostrar lo que hay)
        return jsonify({"status": "success", "message": "Regla guardada (Simulado)"})

# --- ENDPOINTS ORIGINALES (RUN, HISTORY, PIPELINES) ---
# ... (Mantén aquí tus rutas /api/run, /api/history, /api/pipelines que ya funcionaban) ...
# Copia las funciones run_etl, get_history, create_pipeline, health del archivo anterior aquí abajo.

@app.route('/api/run', methods=['POST'])
def run_etl():
    try:
        logger.info(" Ejecución manual solicitada")
        engine = ETLEngine()
        engine.run_pipeline()
        return jsonify({"status": "success", "message": "Pipeline ejecutado."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    try:
        etl = ETLEngine()
        with etl.engine_qa.connect() as conn:
            result = conn.execute(text("SELECT fecha_ejecucion, tabla, registros_procesados, estado, mensaje FROM auditoria ORDER BY fecha_ejecucion DESC LIMIT 50"))
            logs = [{"fecha": str(r[0]), "tabla": r[1], "registros": r[2], "estado": r[3], "mensaje": r[4]} for r in result]
        return jsonify(logs)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/pipelines', methods=['POST'])
def create_pipeline():
    """Crea un pipeline analizando automáticamente la estructura de la tabla"""
    try:
        data = request.json
        tabla = data.get('table')
        nombre = data.get('name') or f"Migración {tabla.capitalize()}"

        config = load_config()
        prod_uri = os.getenv(config['databases']['source_db_env_var'])
        
        # 1. Inspeccionar columnas de la tabla real
        engine = create_engine(prod_uri)
        inspector = inspect(engine)
        columns = inspector.get_columns(tabla)
        pk_constraint = inspector.get_pk_constraint(tabla)
        
        # 2. Detectar PK automáticamente
        pk = "id" # Default
        if pk_constraint and pk_constraint['constrained_columns']:
            pk = pk_constraint['constrained_columns'][0]

        # 3. Detectar reglas inteligentes basadas en nombres de columna
        masking_rules = {}
        filter_col = None
        
        for col in columns:
            col_name = col['name'].lower()
            
            # Detección de fecha para incremental
            if not filter_col and ('fecha' in col_name or 'date' in col_name or 'time' in col_name):
                filter_col = col['name']
            
            # Reglas automáticas
            if 'email' in col_name or 'correo' in col_name:
                masking_rules[col['name']] = 'hash_email'
            elif 'telef' in col_name or 'phone' in col_name:
                masking_rules[col['name']] = 'preserve_format'
            elif 'nomb' in col_name or 'name' in col_name:
                masking_rules[col['name']] = 'fake_name'
            elif 'direc' in col_name or 'address' in col_name or 'calle' in col_name:
                masking_rules[col['name']] = 'redact'
            elif 'tarjeta' in col_name or 'card' in col_name or 'precio' in col_name:
                masking_rules[col['name']] = 'redact'

        # 4. Guardar configuración
        new_job = {
            "name": tabla,
            "pk": pk,
            "filter_column": filter_col, # Puede ser null si no detectó fecha
            "masking_rules": masking_rules
        }
        
        config['tables'].append(new_job)
        save_config(config)
        
        return jsonify({
            "status": "success", 
            "message": f"Pipeline auto-configurado para '{tabla}'. {len(masking_rules)} reglas aplicadas."
        }), 201

    except Exception as e:
        logger.error(f"Error auto-config: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
        
if __name__ == '__main__':
    print(" Servidor Full-Stack API corriendo en http://localhost:5000")
    # Scheduler (opcional si quieres mantenerlo)
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: ETLEngine().run_pipeline(), 'interval', minutes=5)
    scheduler.start()
    app.run(debug=True, port=5000, use_reloader=False)