import os
import yaml
import hashlib
import logging
import pandas as pd
import time
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from faker import Faker
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLEngine:
    def __init__(self):
        config_path = os.path.join(BASE_DIR, 'config.yaml')
        # CORRECCIÓN: Agregar encoding='utf-8'
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.faker = Faker('es_MX')
        self.salt = os.getenv("HASH_SALT", "default_salt").encode()
        self.max_retries = 3  
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

    def log_audit(self, table, records, status, error=None):
        try:
            with self.engine_qa.connect() as conn:
                conn.execute(text("""
                    INSERT INTO auditoria (tabla, registros_procesados, estado, mensaje, fecha_ejecucion)
                    VALUES (:t, :r, :s, :m, :f)
                """), {
                    "t": table, "r": records, "s": status, 
                    "m": str(error)[:500] if error else "OK", "f": datetime.now()
                })
                conn.commit()
        except Exception as e:
            logger.error(f"[WARNING] Fallo en auditoria: {e}")

    def save_json_report(self, table, status, records, mode, error_msg=None):
        try:
            enabled = self.config.get('settings', {}).get('notifications', {}).get('enabled', False)
            if not enabled: return

            report_path = os.path.join(BASE_DIR, 'notifications_log.json')
            
            new_event = {
                "timestamp": datetime.now().isoformat(),
                "table": table,
                "status": status,
                "mode": mode,
                "records": records,
                "details": error_msg if error_msg else "OK"
            }

            history = []
            if os.path.exists(report_path):
                try:
                    # CORRECCIÓN: Agregar encoding='utf-8'
                    with open(report_path, 'r', encoding='utf-8') as f: history = json.load(f)
                except: history = []

            history.insert(0, new_event)
            
            # CORRECCIÓN: Agregar encoding='utf-8'
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(history[:50], f, indent=4, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Error guardando JSON: {e}")

    def get_max_date(self, table, col):
        try:
            with self.engine_qa.connect() as conn:
                return conn.execute(text(f"SELECT MAX({col}) FROM {table}")).scalar()
        except Exception: return None

    def process_table(self, table_conf, override_percent=None):
        table = table_conf['name']
        pk = table_conf['pk']
        filter_col = table_conf.get('filter_column')
        
        if override_percent is not None:
            sample_percent = float(override_percent)
        else:
            sample_percent = table_conf.get('sample_percent', 100)
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"[INFO] Procesando {table} (Intento {attempt}/{self.max_retries})...")
                
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

                # Aplicar Muestreo
                if sample_percent < 100:
                    df = df.sample(frac=sample_percent/100, random_state=42)
                    logger.info(f"   [SAMPLE] Muestreo aplicado: {sample_percent}% ({len(df)} de {total_extracted} registros).")
                else:
                    logger.info(f"   [INFO] {table}: {len(df)} registros nuevos ({mode}).")

                for col, rule in table_conf.get('masking_rules', {}).items():
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: self.mask_value(x, rule))

                with self.engine_qa.connect() as conn:
                    # 1. Desactivar FKs para evitar errores en carga parcial
                    conn.execute(text("SET session_replication_role = 'replica';"))
                    
                    if not df.empty:
                        # Limpieza de duplicados
                        ids_list = df[pk].tolist()
                        if ids_list:
                            safe_ids = [str(x) for x in ids_list]
                            id_str = ",".join(safe_ids)
                            
                            delete_query = f"DELETE FROM {table} WHERE {pk} IN ({id_str})"
                            conn.execute(text(delete_query))
                    
                    if not df.empty:
                        df.to_sql(table, conn, if_exists='append', index=False, method='multi', chunksize=1000)
                    
                    # 2. Restaurar FKs
                    conn.execute(text("SET session_replication_role = 'origin';"))
                    conn.commit()
                
                self.log_audit(table, len(df), f"SUCCESS - {mode}")
                self.save_json_report(table, "SUCCESS", len(df), mode)
                
                logger.info(f"[OK] {table}: Sincronizacion completada.")
                return 

            except Exception as e:
                logger.error(f"[WARN] Fallo en intento {attempt}: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait)
                else:
                    logger.error(f"[ERROR] {table}: Se agotaron los reintentos.")
                    self.log_audit(table, 0, "ERROR", str(e))
                    self.save_json_report(table, "ERROR", 0, "FAILED", str(e))

    def run_pipeline(self, target_table=None, override_percent=None):
        logger.info("[START] Iniciando Pipeline...")
        for table_conf in self.config['tables']:
            if target_table and table_conf['name'] != target_table: continue 
            self.process_table(table_conf, override_percent)

if __name__ == "__main__":
    ETLEngine().run_pipeline()