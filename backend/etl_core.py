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
        self.salt = os.getenv("HASH_SALT", "default_salt").encode()
        self.encryption_key = os.getenv("BACKUP_ENCRYPTION_KEY")
        
        scheduler_conf = settings.get('scheduler', {})
        retry_enabled = scheduler_conf.get('auto_retry', False)
        self.max_retries = 3 if retry_enabled else 1
        self.retry_wait = 2   
        
        try:
            prod_uri = os.getenv(self.config['databases']['source_db_env_var'])
            qa_uri = os.getenv(self.config['databases']['target_db_env_var'])
            self.engine_prod = create_engine(prod_uri, pool_pre_ping=True)
            self.engine_qa = create_engine(qa_uri, pool_pre_ping=True)
            self.validate_environments()
        except Exception as e:
            logger.error(f"[CRITICAL] Error de inicio: {e}")
            raise

    def validate_environments(self):
        try:
            with self.engine_prod.connect() as conn:
                if conn.execute(text("SELECT to_regclass('public._db_meta')")).scalar():
                    env = conn.execute(text("SELECT value FROM _db_meta WHERE key='env'")).scalar()
                    if env != 'production': raise Exception(f" Origen incorrecto: es '{env}'")

            with self.engine_qa.connect() as conn:
                if conn.execute(text("SELECT to_regclass('public._db_meta')")).scalar():
                    env = conn.execute(text("SELECT value FROM _db_meta WHERE key='env'")).scalar()
                    if env != 'qa': raise Exception(f" Destino incorrecto: es '{env}'")
            
            logger.info(" Validación de entornos correcta.")
        except Exception as e:
            logger.error(str(e))
            raise e

    def mask_value(self, value, rule):
        if value is None: return None
        if rule == 'hash_email':
            return hashlib.sha256(str(value).encode() + self.salt).hexdigest()[:12] + "@anon.com"
        elif rule == 'fake_name': return self.faker.name()
        elif rule == 'preserve_format': return f"+52 ({self.faker.random_int(55,99)}) ***-****"
        elif rule == 'redact': return "****"
        return value

    # --- SQL HELPER ---
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

    def _get_schema_definition(self):
        return """
DROP TABLE IF EXISTS detalle_ordenes, ordenes, inventario, clientes, auditoria, _db_meta CASCADE;
CREATE TABLE clientes (id INTEGER PRIMARY KEY, nombre VARCHAR(100), email VARCHAR(100), telefono VARCHAR(50), direccion VARCHAR(200), fecha_registro TIMESTAMP);
CREATE TABLE inventario (id INTEGER PRIMARY KEY, producto VARCHAR(100) UNIQUE, stock INTEGER, ubicacion VARCHAR(50), fecha_registro TIMESTAMP);
CREATE TABLE ordenes (id INTEGER PRIMARY KEY, cliente_id INTEGER REFERENCES clientes(id), total DECIMAL(10, 2), fecha TIMESTAMP);
CREATE TABLE detalle_ordenes (id INTEGER PRIMARY KEY, orden_id INTEGER REFERENCES ordenes(id), producto VARCHAR(100) REFERENCES inventario(producto), cantidad INTEGER, precio_unitario DECIMAL(10, 2));
CREATE TABLE IF NOT EXISTS auditoria (id SERIAL PRIMARY KEY, fecha_ejecucion TIMESTAMP, tabla VARCHAR(50), registros_procesados INTEGER, estado VARCHAR(100), mensaje TEXT, id_ejecucion VARCHAR(50), operacion VARCHAR(20), reglas_aplicadas TEXT, fecha_inicio TIMESTAMP, fecha_fin TIMESTAMP);
CREATE TABLE _db_meta (key VARCHAR PRIMARY KEY, value VARCHAR);
"""

    def create_encrypted_backup(self):
        if not self.encryption_key: raise ValueError("Falta BACKUP_ENCRYPTION_KEY")
        try:
            backup_dir = os.path.join(BASE_DIR, 'backups')
            if not os.path.exists(backup_dir): os.makedirs(backup_dir)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"backup_{ts}.sql.enc"
            filepath = os.path.join(backup_dir, filename)
            
            full_sql = f"-- RESPALDO {ts}\n" + self._get_schema_definition()
            full_sql += "\n-- PROD\n"
            for t in ['_db_meta', 'clientes', 'inventario', 'ordenes', 'detalle_ordenes']: full_sql += self._generate_table_sql(self.engine_prod, t)
            full_sql += "\n-- QA\n"
            for t in ['auditoria']: full_sql += self._generate_table_sql(self.engine_qa, t)

            fernet = Fernet(self.encryption_key.encode())
            with open(filepath, 'wb') as f: f.write(fernet.encrypt(full_sql.encode('utf-8')))
            return filename
        except Exception as e:
            logger.error(f"Error backup: {e}")
            raise e

    def cleanup_old_logs(self):
        try:
            days = self.config.get('settings', {}).get('security', {}).get('log_retention_days', 90)
            cutoff = datetime.now() - timedelta(days=int(days))
            with self.engine_qa.connect() as conn:
                conn.execute(text("DELETE FROM auditoria WHERE fecha_ejecucion < :c"), {"c": cutoff})
                conn.commit()
        except: pass

    # --- NUEVO: log_audit AHORA RECIBE LOS CAMPOS FALTANTES ---
    def log_audit(self, table, records, status, error=None, start_time=None, end_time=None, execution_id=None, operation=None, rules=None):
        try:
            with self.engine_qa.connect() as conn:
                conn.execute(text("""
                    INSERT INTO auditoria (
                        tabla, registros_procesados, estado, mensaje, 
                        fecha_ejecucion, fecha_inicio, fecha_fin,
                        id_ejecucion, operacion, reglas_aplicadas
                    )
                    VALUES (:t, :r, :s, :m, :f, :fi, :ff, :eid, :op, :rules)
                """), {
                    "t": table, "r": records, "s": status, "m": str(error)[:500] if error else "OK", 
                    "f": datetime.now(), "fi": start_time, "ff": end_time,
                    "eid": execution_id, "op": operation, "rules": rules
                })
                conn.commit()
        except Exception as e:
            logger.error(f"Fallo audit DB: {e}")

    def save_json_report(self, table, status, records, mode, error_msg=None):
        try:
            if not self.config.get('settings', {}).get('notifications', {}).get('enabled', False): return
            path = os.path.join(BASE_DIR, 'notifications_log.json')
            event = { "timestamp": datetime.now().isoformat(), "table": table, "status": status, "mode": mode, "records": records, "details": error_msg or "OK" }
            history = []
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f: history = json.load(f)
            history.insert(0, event)
            with open(path, 'w', encoding='utf-8') as f: json.dump(history[:1000], f, indent=4)
        except: pass

    def get_max_date(self, table, col):
        try:
            with self.engine_qa.connect() as conn: return conn.execute(text(f"SELECT MAX({col}) FROM {table}")).scalar()
        except: return None

    # --- NUEVO: Se actualiza process_table para pasar los datos ---
    def process_table(self, table_conf, override_percent=None, execution_id=None):
        table = table_conf['name']
        pk = table_conf['pk']
        filter_col = table_conf.get('filter_column')
        sample_percent = float(override_percent) if override_percent is not None else table_conf.get('sample_percent', 100)
        
        # Preparamos los datos para la auditoría
        masking_rules = table_conf.get('masking_rules', {})
        rules_str = json.dumps(masking_rules, ensure_ascii=False) if masking_rules else "Ninguna"
        operation_type = "ETL_FULL"
        
        for attempt in range(1, self.max_retries + 1):
            start_time = datetime.now()
            try:
                logger.info(f"[INFO] Procesando {table}...")
                
                last_date = self.get_max_date(table, filter_col)
                query = f"SELECT * FROM {table}"
                if last_date and filter_col:
                    query += f" WHERE {filter_col} > '{last_date}'"
                    operation_type = "ETL_INCREMENTAL"
                
                df = pd.read_sql(query, self.engine_prod)
                if df.empty: 
                    logger.info(f"   [SKIP] {table}: Sin novedades.")
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
                
                # GUARDAR CON TODOS LOS DATOS
                self.log_audit(
                    table=table, 
                    records=len(df), 
                    status=f"SUCCESS - {operation_type}", 
                    error=None, 
                    start_time=start_time, 
                    end_time=end_time,
                    execution_id=execution_id,  
                    operation=operation_type,   
                    rules=rules_str             
                )
                self.save_json_report(table, "SUCCESS", len(df), operation_type)
                logger.info(f"[OK] {table}")
                return 

            except Exception as e:
                end_time = datetime.now()
                logger.error(f"[ERROR] {table}: {e}")
                if attempt == self.max_retries:
                    self.log_audit(table, 0, "ERROR", str(e), start_time, end_time, execution_id, operation_type, rules_str)
                    self.save_json_report(table, "ERROR", 0, "FAILED", str(e))
                else: time.sleep(self.retry_wait)

    def run_pipeline(self, target_table=None, override_percent=None):
        logger.info(f"[START] Iniciando Pipeline ({self.app_name})...")
        self.cleanup_old_logs()
        
        # Generamos un ID único para esta ejecución completa
        execution_id = str(uuid.uuid4())
        
        for table_conf in self.config['tables']:
            is_active = table_conf.get('active', True)
            is_forced = (target_table == table_conf['name'])
            
            if not is_active and not is_forced:
                continue

            if target_table and table_conf['name'] != target_table: continue 
            
            # Pasamos el ID a cada tabla
            self.process_table(table_conf, override_percent, execution_id)

if __name__ == "__main__":
    ETLEngine().run_pipeline()