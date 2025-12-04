import yaml
import os
import logging
import atexit
import time
from datetime import datetime
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from etl_core import ETLEngine
from sqlalchemy import text, create_engine, inspect
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

# Importamos la función refactorizada
from init_db import generate_source_data

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

scheduler = BackgroundScheduler()

def load_config():
    path = os.path.join(BASE_DIR, 'config.yaml')
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def save_config(config):
    path = os.path.join(BASE_DIR, 'config.yaml')
    with open(path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, sort_keys=False, allow_unicode=True)

def scheduled_job():
    try:
        config = load_config()
        if config.get('settings', {}).get('scheduler', {}).get('enabled', True):
            logger.info("[CRON] Ejecutando tarea programada...")
            ETLEngine().run_pipeline()
        else:
            logger.info("[CRON] Tarea omitida.")
    except Exception as e:
        logger.error(f"Error en Cron: {e}")

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard():
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
            
            chart_data = [{"name": r[0], "value": r[1]} for r in conn.execute(text("SELECT tabla, SUM(registros_procesados) as total FROM auditoria GROUP BY tabla ORDER BY total DESC LIMIT 5"))]
            
            recent_activity = [{
                "table": r[0], 
                "status": "success" if "SUCCESS" in r[1] else "error", 
                "time": str(r[2]), 
                "records": r[3]
            } for r in conn.execute(text("SELECT tabla, estado, fecha_ejecucion, registros_procesados FROM auditoria ORDER BY fecha_ejecucion DESC LIMIT 5"))]

        system_status = { "api": "online", "scheduler": "running", "db_prod": "unknown", "db_qa": "connected" }
        try:
            prod_uri = os.getenv(config['databases']['source_db_env_var'])
            create_engine(prod_uri).connect().close()
            system_status['db_prod'] = "connected"
        except:
            system_status['db_prod'] = "disconnected"

        return jsonify({
            "kpi": { "pipelines": total_pipelines, "rules": total_rules, "records": res_total, "success_rate": success_rate },
            "chart_data": chart_data, "recent_activity": recent_activity, "system_status": system_status
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- ENDPOINT DE GENERACIÓN DE DATOS ---
@app.route('/api/source/seed', methods=['POST'])
def seed_source_data():
    try:
        # Recibimos: { "productos": 30, "clientes": 50, "ordenes": 100, "detalles": 300 }
        counts = request.json 
        logger.info(f"Generando datos semilla: {counts}")
        
        generate_source_data(counts)
        
        return jsonify({"status": "success", "message": "Datos regenerados correctamente."}), 200
    except Exception as e:
        logger.error(f"Error generando datos: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/connections', methods=['GET', 'POST', 'DELETE'])
def handle_connections():
    config = load_config()
    
    if request.method == 'GET':
        connections = []
        registry = config['databases'].get('registry', {
            "prod": {"name": "Producción", "env_var": "SUPABASE_PROD_URI"},
            "qa": {"name": "QA", "env_var": "SUPABASE_QA_URI"}
        })
        
        for db_id, db_info in registry.items():
            uri = os.getenv(db_info['env_var'])
            status, host, latency, version = "disconnected", "unknown", 0, "Unknown"
            
            if uri:
                try:
                    if '@' in uri:
                        host = uri.split('@')[1].split(':')[0]
                    start = time.time()
                    engine = create_engine(uri)
                    with engine.connect() as conn:
                        ver = conn.execute(text("SHOW server_version")).scalar()
                        version = f"PG {ver}"
                    latency = round((time.time() - start) * 1000)
                    status = "connected"
                except:
                    status = "error"
            
            connections.append({
                "id": db_id, "name": db_info['name'], "host": host, "status": status,
                "isProduction": db_id == config['databases'].get('active_source', 'prod'),
                "latency": latency, "version": version, "lastChecked": datetime.now().isoformat()
            })
        return jsonify(connections)

    if request.method == 'POST':
        try:
            data = request.json
            name = data.get('name')
            uri = data.get('uri')
            if not name or not uri: return jsonify({"error": "Datos faltantes"}), 400
            
            conn_id = name.lower().replace(" ", "_")
            env_var = f"DB_{conn_id.upper()}_URI"
            
            with open(os.path.join(BASE_DIR, '.env'), 'a', encoding='utf-8') as f:
                f.write(f"\n{env_var}={uri}")
            
            load_dotenv(os.path.join(BASE_DIR, '.env'), override=True)
            
            if 'registry' not in config['databases']:
                config['databases']['registry'] = {}
            
            config['databases']['registry'][conn_id] = {"name": name, "env_var": env_var, "type": "postgresql"}
            save_config(config)
            return jsonify({"status": "success"}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    if request.method == 'DELETE':
        try:
            cid = request.json.get('id')
            protected = ['prod', 'qa', config['databases'].get('active_source'), config['databases'].get('active_target')]
            if cid in protected:
                return jsonify({"error": "Protegida"}), 403
            
            if cid in config['databases'].get('registry', {}):
                del config['databases']['registry'][cid]
                save_config(config)
                return jsonify({"status": "success"}), 200
            return jsonify({"error": "No existe"}), 404
        except:
            return jsonify({"error": "Error interno"}), 500

@app.route('/api/settings', methods=['GET', 'POST'])
def handle_settings():
    if request.method == 'GET':
        return jsonify(load_config().get('settings', {}))
    
    if request.method == 'POST':
        try:
            new_data = request.json
            config = load_config()
            
            for k in ['app_name', 'batch_size', 'extraction_window_days']:
                if k in new_data: config['settings'][k] = new_data[k]
            
            for section in ['notifications', 'security', 'scheduler']:
                if section in new_data:
                    config['settings'].setdefault(section, {}).update(new_data[section])
            
            save_config(config)

            try:
                new_interval = int(config['settings']['scheduler'].get('interval_minutes', 5))
                scheduler.reschedule_job('etl_job', trigger='interval', minutes=new_interval)
                logger.info(f"Scheduler actualizado a {new_interval}m")
            except:
                pass

            return jsonify({"status": "success"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

@app.route('/api/pipelines', methods=['GET', 'POST'])
def handle_pipelines():
    config = load_config()
    
    if request.method == 'GET':
        try:
            pipelines = []
            try:
                engine = ETLEngine().engine_qa
                with engine.connect() as conn:
                    for t in config['tables']:
                        last = conn.execute(text(f"SELECT estado, fecha_ejecucion, registros_procesados FROM auditoria WHERE tabla='{t['name']}' ORDER BY fecha_ejecucion DESC LIMIT 1")).fetchone()
                        status, date, recs = ("idle", None, 0)
                        if last:
                            status = "success" if "SUCCESS" in last[0] else "error"
                            date, recs = str(last[1]), last[2]
                        
                        pct = t.get('sample_percent', 100)
                        desc = t.get('description', f"Migración {t['name']}")
                        if pct < 100:
                            desc += f" (Muestra: {pct}%)"

                        pipelines.append({
                            "id": t['name'],
                            "name": desc,
                            "sourceDb": "Prod",
                            "targetDb": "QA",
                            "status": status,
                            "lastRun": date,
                            "tablesCount": 1,
                            "maskingRulesCount": len(t.get('masking_rules', {})),
                            "recordsProcessed": recs
                        })
            except: 
                 pipelines = [{"id": t['name'], "name": t['name'], "status": "error"} for t in config['tables']]
                 
            return jsonify(pipelines)
        except:
            return jsonify([])

    if request.method == 'POST':
        data = request.json
        if any(t['name'] == data['table'] for t in config['tables']):
            return jsonify({"error": "Existe"}), 409
        
        try:
            prod_uri = os.getenv(config['databases']['source_db_env_var'])
            cols = inspect(create_engine(prod_uri)).get_columns(data['table'])
            
            filter_col = next((c['name'] for c in cols if 'fecha' in c['name'].lower() or 'date' in c['name'].lower()), None)
            masking = {}
            for c in cols:
                cn = c['name'].lower()
                if 'email' in cn: masking[c['name']] = 'hash_email'
                elif 'telef' in cn: masking[c['name']] = 'preserve_format'
                elif 'nombre' in cn: masking[c['name']] = 'fake_name'
                elif 'direc' in cn: masking[c['name']] = 'redact'
            
            config['tables'].append({
                "name": data['table'], "description": data.get('name'), 
                "pk": "id", "filter_column": filter_col, 
                "sample_percent": data.get('percentage', 100),
                "masking_rules": masking
            })
            save_config(config)
            return jsonify({"status": "success"}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

@app.route('/api/rules', methods=['GET', 'POST', 'DELETE'])
def handle_rules():
    config_path = os.path.join(BASE_DIR, 'config.yaml')
    if request.method == 'GET':
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            rules = []
            for t in config.get('tables', []):
                for c, r in t.get('masking_rules', {}).items():
                    rules.append({"id": f"{t['name']}-{c}", "name": f"{c} ({r})", "table": t['name'], "column": c, "type": r, "isActive": True})
            return jsonify(rules)
        except:
            return jsonify([])
    
    if request.method == 'POST':
        data = request.json
        config = load_config()
        for t in config['tables']:
            if t['name'] == data['table']:
                t.setdefault('masking_rules', {})[data['column']] = data['type']
        save_config(config)
        return jsonify({"status": "success"})

    if request.method == 'DELETE':
        data = request.json
        config = load_config()
        for t in config['tables']:
            if t['name'] == data['table'] and data['column'] in t.get('masking_rules', {}):
                del t['masking_rules'][data['column']]
        save_config(config)
        return jsonify({"status": "success"})

@app.route('/api/rules/reset', methods=['POST'])
def reset_rules():
    config = load_config()
    defaults = {
        "clientes": {"nombre": "fake_name", "email": "hash_email", "telefono": "preserve_format", "direccion": "redact"},
        "ordenes": {"total": "none"},
        "detalle_ordenes": {"producto": "hash_email", "precio_unitario": "none"},
        "inventario": {"producto": "hash_email", "ubicacion": "redact"}
    }
    for t in config['tables']:
        if t['name'] in defaults:
            t['masking_rules'] = defaults[t['name']]
    save_config(config)
    return jsonify({"status": "success"})

@app.route('/api/source/tables', methods=['GET'])
def get_source_tables():
    try:
        config = load_config()
        prod_uri = os.getenv(config['databases']['source_db_env_var'])
        return jsonify(inspect(create_engine(prod_uri)).get_table_names())
    except:
        return jsonify([])

@app.route('/api/source/columns/<table_name>', methods=['GET'])
def get_cols(table_name):
    try:
        config = load_config()
        prod_uri = os.getenv(config['databases']['source_db_env_var'])
        return jsonify([c['name'] for c in inspect(create_engine(prod_uri)).get_columns(table_name)])
    except:
        return jsonify([])

@app.route('/api/run', methods=['POST'])
def run_etl():
    try:
        target = request.json.get('table')
        percentage = request.json.get('percentage')
        logger.info(f"Ejecucion manual: {target or 'TODO'}. Muestreo: {percentage if percentage else '100'}%")
        ETLEngine().run_pipeline(target_table=target, override_percent=percentage)
        return jsonify({"status": "success", "message": "Ejecutado correctamente"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    try:
        with ETLEngine().engine_qa.connect() as conn:
            res = conn.execute(text("SELECT fecha_ejecucion, tabla, registros_procesados, estado, mensaje FROM auditoria ORDER BY fecha_ejecucion DESC LIMIT 50"))
            return jsonify([{"fecha": str(r[0]), "tabla": r[1], "registros": r[2], "estado": r[3], "mensaje": r[4]} for r in res])
    except:
        return jsonify([])

@app.route('/api/notifications/report', methods=['GET'])
def download_report():
    try:
        report_path = os.path.join(BASE_DIR, 'notifications_log.json')
        if not os.path.exists(report_path): 
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write('[]')
        return send_file(report_path, as_attachment=True, download_name='reporte_auditoria.json')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "online"}), 200

if __name__ == '__main__':
    print("Servidor Maestro listo en http://localhost:5000")
    try:
        conf = load_config()
        initial_interval = int(conf.get('settings', {}).get('scheduler', {}).get('interval_minutes', 5))
    except:
        initial_interval = 5

    scheduler.add_job(func=scheduled_job, trigger='interval', minutes=initial_interval, id='etl_job')
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
    app.run(debug=True, port=5000, use_reloader=False)