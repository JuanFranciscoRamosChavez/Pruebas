import os
import yaml
import hashlib
import logging
import pandas as pd
import time
import json
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
        except Exception as e:
            logger.error(f"[CRITICAL] Error de configuracion: {e}")
            raise

    def mask_value(self, value, rule):
        if value is None: return None
        if rule == 'hash_email':
            return hashlib.sha256(str(value).encode() + self.salt).hexdigest()[:12] + "@anon.com"
        elif rule == 'fake_name': return self.faker.name()
        elif rule == 'preserve_format': return f"+52 ({self.faker.random_int(55,99)}) ***-****"
        elif rule == 'redact': return "****"
        return value

    # --- DEFINICIÓN DEL ESQUEMA (ESTRUCTURA) ---
    def _get_schema_definition(self):
        """Devuelve el SQL para recrear las tablas."""
        return """
-- ===========================
-- ESTRUCTURA DE LA BASE DE DATOS
-- ===========================
DROP TABLE IF EXISTS detalle_ordenes, ordenes, inventario, clientes, auditoria CASCADE;

CREATE TABLE clientes (
    id INTEGER PRIMARY KEY,
    nombre VARCHAR(100),
    email VARCHAR(100),
    telefono VARCHAR(50),
    direccion VARCHAR(200),
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE inventario (
    id INTEGER PRIMARY KEY,
    producto VARCHAR(100) UNIQUE,
    stock INTEGER,
    ubicacion VARCHAR(50),
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ordenes (
    id INTEGER PRIMARY KEY,
    cliente_id INTEGER REFERENCES clientes(id),
    total DECIMAL(10, 2),
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE detalle_ordenes (
    id INTEGER PRIMARY KEY,
    orden_id INTEGER REFERENCES ordenes(id),
    producto VARCHAR(100) REFERENCES inventario(producto),
    cantidad INTEGER,
    precio_unitario DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS auditoria (
    id SERIAL PRIMARY KEY, 
    fecha_ejecucion TIMESTAMP, 
    tabla VARCHAR(50), 
    registros_procesados INTEGER, 
    estado VARCHAR(100), 
    mensaje TEXT, 
    id_ejecucion VARCHAR(50), 
    operacion VARCHAR(20), 
    reglas_aplicadas TEXT, 
    fecha_inicio TIMESTAMP, 
    fecha_fin TIMESTAMP
);

-- ===========================
-- INICIO DE DATOS
-- ===========================
"""

    def _generate_table_sql(self, engine, table_name):
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
            if df.empty:
                return f"\n-- Tabla {table_name} sin datos.\n"
            
            sql_lines = [f"\n-- Datos de la tabla: {table_name}"]
            
            for _, row in df.iterrows():
                values = []
                for val in row:
                    if pd.isna(val):
                        values.append("NULL")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    else:
                        clean_str = str(val).replace("'", "''")
                        values.append(f"'{clean_str}'")
                
                vals_str = ", ".join(values)
                sql_lines.append(f"INSERT INTO {table_name} VALUES ({vals_str});")
            
            return "\n".join(sql_lines) + "\n"
        except Exception as e:
            logger.error(f"Error generando SQL para {table_name}: {e}")
            return f"\n-- Error exportando {table_name}: {str(e)}\n"

    def create_encrypted_backup(self):
        if not self.encryption_key:
            raise ValueError("Falta BACKUP_ENCRYPTION_KEY en el archivo .env")

        try:
            backup_dir = os.path.join(BASE_DIR, 'backups')
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"backup_{timestamp}.sql.enc"
            filepath = os.path.join(backup_dir, filename)

            logger.info(" Generando script SQL completo...")
            
            # 1. Cabecera y Estructura
            full_sql = f"-- RESPALDO CIFRADO DATAMASK ETL\n-- Fecha: {timestamp}\n-- App: {self.app_name}\n"
            full_sql += self._get_schema_definition() # <--- AQUI AGREGAMOS LA ESTRUCTURA
            
            # 2. Datos de Producción (Como ejemplo de semilla)
            # Nota: Si quisieras restaurar SOLO QA, podrías comentar esta parte o separarla.
            # Por ahora guardamos todo en el mismo script.
            full_sql += "\n-- ORIGEN: DATOS DE PRODUCCION (SEMILLA)\n"
            for table in ['clientes', 'inventario', 'ordenes', 'detalle_ordenes']:
                full_sql += self._generate_table_sql(self.engine_prod, table)
            
            # 3. Datos de QA (Incluyendo historial)
            full_sql += "\n-- ORIGEN: DATOS DE QA (RESULTADOS)\n"
            # Ojo: 'auditoria' solo existe en QA
            for table in ['auditoria']:
                full_sql += self._generate_table_sql(self.engine_qa, table)

            # 4. Encriptar
            logger.info(" Cifrando...")
            fernet = Fernet(self.encryption_key.encode())
            encrypted_data = fernet.encrypt(full_sql.encode('utf-8'))

            with open(filepath, 'wb') as f:
                f.write(encrypted_data)
            
            logger.info(f" Respaldo listo: {filename}")
            return filename

        except Exception as e:
            logger.error(f" Fallo crítico en backup: {e}")
            raise e

    def cleanup_old_logs(self):
        try:
            days = self.config.get('settings', {}).get('security', {}).get('log_retention_days', 90)
            cutoff_date = datetime.now() - timedelta(days=int(days))
            with self.engine_qa.connect() as conn:
                conn.execute(text("DELETE FROM auditoria WHERE fecha_ejecucion < :cutoff"), {"cutoff": cutoff_date})
                conn.commit()
        except Exception: pass

    def log_audit(self, table, records, status, error=None, start_time=None, end_time=None):
        try:
            with self.engine_qa.connect() as conn:
                conn.execute(text("""
                    INSERT INTO auditoria (tabla, registros_procesados, estado, mensaje, fecha_ejecucion, fecha_inicio, fecha_fin)
                    VALUES (:t, :r, :s, :m, :f, :fi, :ff)
                """), {
                    "t": table, "r": records, "s": status, 
                    "m": str(error)[:500] if error else "OK", 
                    "f": datetime.now(), "fi": start_time, "ff": end_time
                })
                conn.commit()
        except Exception as e:
            logger.error(f"[WARNING] Fallo en auditoria: {e}")

    def save_json_report(self, table, status, records, mode, error_msg=None):
        try:
            enabled = self.config.get('settings', {}).get('notifications', {}).get('enabled', False)
            if not enabled: return

            report_path = os.path.join(BASE_DIR, 'notifications_log.json')
            days = self.config.get('settings', {}).get('security', {}).get('log_retention_days', 90)
            cutoff_date = datetime.now() - timedelta(days=int(days))

            new_event = {
                "timestamp": datetime.now().isoformat(),
                "table": table, "status": status, "mode": mode, "records": records, "details": error_msg or "OK"
            }

            history = []
            if os.path.exists(report_path):
                with open(report_path, 'r', encoding='utf-8') as f: history = json.load(f)

            history.insert(0, new_event)
            
            filtered_history = [
                entry for entry in history 
                if datetime.fromisoformat(entry['timestamp']) > cutoff_date
            ][:1000] 

            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(filtered_history, f, indent=4, ensure_ascii=False)
        except Exception: pass

    def get_max_date(self, table, col):
        try:
            with self.engine_qa.connect() as conn:
                return conn.execute(text(f"SELECT MAX({col}) FROM {table}")).scalar()
        except Exception: return None

    def process_table(self, table_conf, override_percent=None):
        table = table_conf['name']
        pk = table_conf['pk']
        filter_col = table_conf.get('filter_column')
        
        sample_percent = float(override_percent) if override_percent is not None else table_conf.get('sample_percent', 100)
        
        for attempt in range(1, self.max_retries + 1):
            start_time = datetime.now()
            try:
                retry_msg = f" (Intento {attempt}/{self.max_retries})" if self.max_retries > 1 else ""
                logger.info(f"[INFO] Procesando {table}{retry_msg}...")
                
                last_date = self.get_max_date(table, filter_col)
                query = f"SELECT * FROM {table}"
                mode = "FULL"
                
                if last_date and filter_col:
                    query += f" WHERE {filter_col} > '{last_date}'"
                    mode = "INCREMENTAL"
                
                df = pd.read_sql(query, self.engine_prod)
                
                if df.empty:
                    logger.info(f"   [SKIP] {table}: Sin novedades.")
                    return 

                total_extracted = len(df)

                if sample_percent < 100:
                    df = df.sample(frac=sample_percent/100, random_state=42)
                    logger.info(f"   [SAMPLE] Muestreo aplicado: {sample_percent}% ({len(df)} de {total_extracted} registros).")
                else:
                    logger.info(f"   [INFO] {table}: {len(df)} registros nuevos ({mode}).")

                for col, rule in table_conf.get('masking_rules', {}).items():
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: self.mask_value(x, rule))

                with self.engine_qa.connect() as conn:
                    conn.execute(text("SET session_replication_role = 'replica';"))
                    
                    if not df.empty:
                        ids_list = df[pk].tolist()
                        if ids_list:
                            safe_ids = [str(x) for x in ids_list]
                            conn.execute(text(f"DELETE FROM {table} WHERE {pk} IN ({','.join(safe_ids)})"))
                    
                    if not df.empty:
                        df.to_sql(table, conn, if_exists='append', index=False, method='multi', chunksize=self.batch_size)
                    
                    conn.execute(text("SET session_replication_role = 'origin';"))
                    conn.commit()
                
                end_time = datetime.now()
                self.log_audit(table, len(df), f"SUCCESS - {mode}", None, start_time, end_time)
                self.save_json_report(table, "SUCCESS", len(df), mode)
                
                logger.info(f"[OK] {table}: Sincronizacion completada en {(end_time - start_time).total_seconds():.2f}s.")
                return 

            except Exception as e:
                end_time = datetime.now()
                logger.error(f"[WARN] Fallo en intento {attempt}: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait)
                else:
                    logger.error(f"[ERROR] {table}: Se agotaron los reintentos.")
                    self.log_audit(table, 0, "ERROR", str(e), start_time, end_time)
                    self.save_json_report(table, "ERROR", 0, "FAILED", str(e))

    def run_pipeline(self, target_table=None, override_percent=None):
        logger.info(f"[START] Iniciando Pipeline ({self.app_name})...")
        self.cleanup_old_logs()
        for table_conf in self.config['tables']:
            if target_table and table_conf['name'] != target_table: continue 
            self.process_table(table_conf, override_percent)

if __name__ == "__main__":
    ETLEngine().run_pipeline()