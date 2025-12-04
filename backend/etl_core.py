import os
import yaml
import hashlib
import logging
import pandas as pd
import time
import json  # <--- IMPORTANTE
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
        with open(config_path, 'r') as f:
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
            logger.error(f"❌ Error crítico de configuración: {e}")
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
            logger.error(f"⚠️ Fallo crítico en auditoría: {e}")

    # --- NUEVA FUNCIÓN: GUARDAR JSON LOCAL ---
    def save_json_report(self, table, status, records, mode, error_msg=None):
        try:
            # Verificar si el usuario activó el reporte en config.yaml
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

            # Leer existente
            history = []
            if os.path.exists(report_path):
                try:
                    with open(report_path, 'r') as f: history = json.load(f)
                except: history = []

            # Agregar al principio
            history.insert(0, new_event)
            
            # Guardar (Máximo 50 últimos eventos)
            with open(report_path, 'w') as f:
                json.dump(history[:50], f, indent=4)
                
        except Exception as e:
            logger.error(f"Error guardando JSON: {e}")

    def get_max_date(self, table, col):
        try:
            with self.engine_qa.connect() as conn:
                return conn.execute(text(f"SELECT MAX({col}) FROM {table}")).scalar()
        except Exception: return None

    def process_table(self, table_conf):
        table = table_conf['name']
        pk = table_conf['pk']
        filter_col = table_conf.get('filter_column')
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"🔄 Procesando {table} (Intento {attempt}/{self.max_retries})...")
                
                last_date = self.get_max_date(table, filter_col)
                query = f"SELECT * FROM {table}"
                mode = "FULL"
                
                if last_date and filter_col:
                    query += f" WHERE {filter_col} > '{last_date}'"
                    mode = "INCREMENTAL"
                
                df = pd.read_sql(query, self.engine_prod)
                
                if df.empty:
                    logger.info(f"   💤 {table}: Sin novedades.")
                    return 

                logger.info(f"   📥 {table}: {len(df)} registros nuevos ({mode}).")

                for col, rule in table_conf.get('masking_rules', {}).items():
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: self.mask_value(x, rule))

                with self.engine_qa.connect() as conn:
                    if not df.empty:
                        ids = tuple(df[pk].tolist())
                        if ids:
                            id_str = str(ids) if len(ids) > 1 else f"({ids[0]})"
                            conn.execute(text(f"DELETE FROM {table} WHERE {pk} IN {id_str}"))
                            conn.commit()
                
                df.to_sql(table, self.engine_qa, if_exists='append', index=False, method='multi', chunksize=1000)
                
                self.log_audit(table, len(df), f"SUCCESS - {mode}")
                
                # --- GUARDAR EN JSON ---
                self.save_json_report(table, "SUCCESS", len(df), mode)
                
                logger.info(f"✅ {table}: Sincronización completada.")
                return 

            except Exception as e:
                logger.error(f"⚠️ Fallo en intento {attempt}: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait)
                else:
                    logger.error(f"❌ {table}: Se agotaron los reintentos.")
                    self.log_audit(table, 0, "ERROR", str(e))
                    # --- GUARDAR ERROR EN JSON ---
                    self.save_json_report(table, "ERROR", 0, "FAILED", str(e))

    def run_pipeline(self, target_table=None):
        logger.info("🚀 Iniciando Pipeline...")
        for table_conf in self.config['tables']:
            if target_table and table_conf['name'] != target_table: continue 
            self.process_table(table_conf)

if __name__ == "__main__":
    ETLEngine().run_pipeline()