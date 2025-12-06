import os
import yaml
import hashlib
import logging
import pandas as pd
import time
import json
import uuid
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from faker import Faker
from dotenv import load_dotenv
from cryptography.fernet import Fernet

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLEngine:
    def __init__(self):
        config_path = os.path.join(BASE_DIR, 'config.yaml')
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        settings = self.config.get('settings', {})
        self.app_name = settings.get('app_name', 'DataMask ETL')
        self.batch_size = int(settings.get('batch_size', 1000))
        self.faker = Faker('es_MX')
        self.salt = os.getenv("HASH_SALT", "default").encode()
        self.encryption_key = os.getenv("BACKUP_ENCRYPTION_KEY")
        self.max_retries = 3 if settings.get('scheduler', {}).get('auto_retry', False) else 1
        self.retry_wait = 2
        
        try:
            self.engine_prod = create_engine(os.getenv(self.config['databases']['source_db_env_var']), pool_pre_ping=True)
            self.engine_qa = create_engine(os.getenv(self.config['databases']['target_db_env_var']), pool_pre_ping=True)
            self.validate_environments()
        except Exception as e:
            logger.error(f"[CRITICAL] Error: {e}")
            raise

    def validate_environments(self):
        try:
            with self.engine_prod.connect() as conn:
                if conn.execute(text("SELECT to_regclass('public._db_meta')")).scalar():
                    if conn.execute(text("SELECT value FROM _db_meta WHERE key='env'")).scalar() != 'production':
                        raise Exception("⛔ Origen NO es Production")
            with self.engine_qa.connect() as conn:
                if conn.execute(text("SELECT to_regclass('public._db_meta')")).scalar():
                    if conn.execute(text("SELECT value FROM _db_meta WHERE key='env'")).scalar() != 'qa':
                        raise Exception("⛔ Destino NO es QA")
            logger.info("✅ Validación de entornos correcta.")
        except Exception as e: raise e

    def mask_value(self, value, rule):
        if value is None: return None
        if rule == 'hash_email': return hashlib.sha256(str(value).encode() + self.salt).hexdigest()[:12] + "@anon.com"
        elif rule == 'fake_name': return self.faker.name()
        elif rule == 'preserve_format': return f"+52 ({self.faker.random_int(55,99)}) ***-****"
        elif rule == 'redact': return "****"
        return value

    def _get_schema_definition(self):
        return """
DROP TABLE IF EXISTS detalle_ordenes, ordenes, inventario, clientes, auditoria, _db_meta CASCADE;
CREATE TABLE clientes (id INTEGER PRIMARY KEY, nombre VARCHAR(100), email VARCHAR(100), telefono VARCHAR(50), direccion VARCHAR(200), fecha_registro TIMESTAMP);
CREATE TABLE inventario (id INTEGER PRIMARY KEY, producto VARCHAR(100) UNIQUE, stock INTEGER, ubicacion VARCHAR(50), fecha_registro TIMESTAMP);
CREATE TABLE ordenes (id INTEGER PRIMARY KEY, cliente_id INTEGER REFERENCES clientes(id), total DECIMAL(10, 2), fecha TIMESTAMP);
CREATE TABLE detalle_ordenes (id INTEGER PRIMARY KEY, orden_id INTEGER REFERENCES ordenes(id), producto VARCHAR(100) REFERENCES inventario(producto), cantidad INTEGER, precio_unitario DECIMAL(10, 2));
CREATE TABLE IF NOT EXISTS auditoria (id SERIAL PRIMARY KEY, id_ejecucion VARCHAR(50), fecha_ejecucion TIMESTAMP, tabla VARCHAR(50), registros_procesados INTEGER, registros_fallidos INTEGER, estado VARCHAR(100), mensaje TEXT, operacion VARCHAR(50), reglas_aplicadas TEXT, fecha_inicio TIMESTAMP, fecha_fin TIMESTAMP);
CREATE TABLE _db_meta (key VARCHAR PRIMARY KEY, value VARCHAR);
"""
    def _generate_table_sql(self, engine, table_name):
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
            if df.empty: return f"\n-- Tabla {table_name} vacía.\n"
            sql = [f"\n-- Datos: {table_name}"]
            for _, row in df.iterrows():
                vals = []
                for val in row:
                    if pd.isna(val): vals.append("NULL")
                    elif isinstance(val, (int, float)): vals.append(str(val))
                    else: vals.append(f"'{str(val).replace("'", "''")}'")
                sql.append(f"INSERT INTO {table_name} VALUES ({', '.join(vals)});")
            return "\n".join(sql) + "\n"
        except: return ""

    def create_encrypted_backup(self):
        if not self.encryption_key: raise ValueError("Falta BACKUP_ENCRYPTION_KEY")
        try:
            backup_dir = os.path.join(BASE_DIR, 'backups')
            if not os.path.exists(backup_dir): os.makedirs(backup_dir)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"backup_{ts}.sql.enc"
            
            full_sql = f"-- RESPALDO {ts}\n" + self._get_schema_definition()
            for t in ['_db_meta', 'clientes', 'inventario', 'ordenes', 'detalle_ordenes']: full_sql += self._generate_table_sql(self.engine_prod, t)
            for t in ['auditoria']: full_sql += self._generate_table_sql(self.engine_qa, t)

            fernet = Fernet(self.encryption_key.encode())
            with open(os.path.join(backup_dir, filename), 'wb') as f: f.write(fernet.encrypt(full_sql.encode('utf-8')))
            return filename
        except Exception as e: raise e

    def cleanup_old_logs(self):
        try:
            days = self.config.get('settings', {}).get('security', {}).get('log_retention_days', 90)
            cutoff = datetime.now() - timedelta(days=int(days))
            with self.engine_qa.connect() as conn: conn.execute(text("DELETE FROM auditoria WHERE fecha_ejecucion < :c"), {"c": cutoff}); conn.commit()
        except: pass

    # --- LOG A BASE DE DATOS ---
    def log_audit(self, table, records, status, error=None, start_time=None, end_time=None, execution_id=None, operation=None, rules=None, failed=0):
        try:
            with self.engine_qa.connect() as conn:
                conn.execute(text("""
                    INSERT INTO auditoria (
                        id_ejecucion, fecha_ejecucion, tabla, registros_procesados, registros_fallidos,
                        estado, mensaje, operacion, reglas_aplicadas, fecha_inicio, fecha_fin
                    ) VALUES (:eid, :f, :t, :r, :rf, :s, :m, :op, :rules, :fi, :ff)
                """), {
                    "eid": execution_id, "f": datetime.now(), "t": table, "r": records, "rf": failed,
                    "s": status, "m": str(error)[:500] if error else "OK", "op": operation, "rules": rules,
                    "fi": start_time, "ff": end_time
                })
                conn.commit()
        except: pass

    # --- LOG A JSON (ESTRUCTURADO PARA EL ASESOR) ---
    def save_json_report(self, table, status, records, mode, error_msg=None, start_time=None, end_time=None, execution_id=None, rules=None, failed=0):
        try:
            if not self.config.get('settings', {}).get('notifications', {}).get('enabled', False): return
            path = os.path.join(BASE_DIR, 'notifications_log.json')
            days = self.config.get('settings', {}).get('security', {}).get('log_retention_days', 90)
            cutoff = datetime.now() - timedelta(days=int(days))
            
            # Estructura Solicitada
            event = {
                "execution_id": execution_id,
                "timestamp": datetime.now().isoformat(),
                "table_name": table,
                "status": status,
                "operation_mode": mode,
                "start_time": start_time.isoformat() if start_time else None,
                "end_time": end_time.isoformat() if end_time else None,
                "records_updated": records,
                "records_failed": failed,
                "masking_applied": rules,
                "details": error_msg or "OK"
            }
            
            history = []
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f: history = json.load(f)
            history.insert(0, event)
            history = [h for h in history if datetime.fromisoformat(h['timestamp']) > cutoff][:1000]
            with open(path, 'w', encoding='utf-8') as f: json.dump(history, f, indent=4)
        except: pass

    def get_max_date(self, table, col):
        try:
            with self.engine_qa.connect() as conn: return conn.execute(text(f"SELECT MAX({col}) FROM {table}")).scalar()
        except: return None

    def process_table(self, table_conf, override_percent=None, execution_id=None):
        table = table_conf['name']
        pk = table_conf['pk']
        filter_col = table_conf.get('filter_column')
        sample_percent = float(override_percent) if override_percent is not None else table_conf.get('sample_percent', 100)
        
        # Datos para auditoría
        masking_rules = table_conf.get('masking_rules', {})
        rules_str = json.dumps(masking_rules) if masking_rules else "None"
        op_mode = "ETL_FULL"

        for attempt in range(1, self.max_retries + 1):
            start_time = datetime.now()
            try:
                logger.info(f"[INFO] Procesando {table}...")
                last_date = self.get_max_date(table, filter_col)
                query = f"SELECT * FROM {table}"
                if last_date and filter_col:
                    query += f" WHERE {filter_col} > '{last_date}'"
                    op_mode = "ETL_INCREMENTAL"
                
                df = pd.read_sql(query, self.engine_prod)
                if df.empty: 
                    logger.info(f"   [SKIP] {table}: Sin cambios.")
                    return 

                if sample_percent < 100: df = df.sample(frac=sample_percent/100, random_state=42)

                for col, rule in masking_rules.items():
                    if col in df.columns: df[col] = df[col].apply(lambda x: self.mask_value(x, rule))

                with self.engine_qa.connect() as conn:
                    conn.execute(text("SET session_replication_role = 'replica';"))
                    if not df.empty:
                        ids = [str(x) for x in df[pk].tolist()]
                        conn.execute(text(f"DELETE FROM {table} WHERE {pk} IN ({','.join(ids)})"))
                    df.to_sql(table, conn, if_exists='append', index=False, method='multi', chunksize=self.batch_size)
                    conn.execute(text("SET session_replication_role = 'origin';"))
                    conn.commit()
                
                end_time = datetime.now()
                self.log_audit(table, len(df), f"SUCCESS", None, start_time, end_time, execution_id, op_mode, rules_str, 0)
                self.save_json_report(table, "SUCCESS", len(df), op_mode, None, start_time, end_time, execution_id, rules_str, 0)
                logger.info(f"[OK] {table}")
                return 

            except Exception as e:
                end_time = datetime.now()
                logger.error(f"[ERROR] {table}: {e}")
                if attempt == self.max_retries:
                    self.log_audit(table, 0, "ERROR", str(e), start_time, end_time, execution_id, op_mode, rules_str, len(df) if 'df' in locals() else 0)
                    self.save_json_report(table, "ERROR", 0, op_mode, str(e), start_time, end_time, execution_id, rules_str, len(df) if 'df' in locals() else 0)
                else: time.sleep(self.retry_wait)

    def run_pipeline(self, target_table=None, override_percent=None):
        logger.info(f"[START] Pipeline ({self.app_name})...")
        self.cleanup_old_logs()
        execution_id = str(uuid.uuid4())
        
        for table_conf in self.config['tables']:
            is_active = table_conf.get('active', True)
            is_forced = (target_table == table_conf['name'])
            
            if not is_active and not is_forced: continue
            if target_table and table_conf['name'] != target_table: continue 
            
            self.process_table(table_conf, override_percent, execution_id)

if __name__ == "__main__":
    ETLEngine().run_pipeline()