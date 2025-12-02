import os
import yaml
import hashlib
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from faker import Faker
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLEngine:
    def __init__(self):
        config_path = os.path.join(BASE_DIR, 'config.yaml')
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.faker = Faker('es_MX')
        self.salt = os.getenv("HASH_SALT", "default_salt").encode()
        
        try:
            prod_uri = os.getenv(self.config['databases']['source_db_env_var'])
            qa_uri = os.getenv(self.config['databases']['target_db_env_var'])
            self.engine_prod = create_engine(prod_uri, pool_pre_ping=True)
            self.engine_qa = create_engine(qa_uri, pool_pre_ping=True)
        except Exception as e:
            logger.error(f" Error conexión: {e}")
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
                """), {"t": table, "r": records, "s": status, "m": str(error)[:200] if error else "OK", "f": datetime.now()})
                conn.commit()
        except Exception as e: logger.error(f" Fallo audit: {e}")

    # --- FUNCIÓN CLAVE PARA INCREMENTAL ---
    def get_max_date(self, table, col):
        try:
            with self.engine_qa.connect() as conn:
                return conn.execute(text(f"SELECT MAX({col}) FROM {table}")).scalar()
        except Exception: return None

    def run_pipeline(self):
        logger.info(" Iniciando Pipeline Inteligente (Delta)...")
        
        for table_conf in self.config['tables']:
            table = table_conf['name']
            pk = table_conf['pk']
            filter_col = table_conf.get('filter_column')
            
            try:
                # 1. DETECTAR DELTA
                last_date = self.get_max_date(table, filter_col)
                query = f"SELECT * FROM {table}"
                
                if last_date and filter_col:
                    logger.info(f" {table}: Última carga {last_date}. Buscando nuevos...")
                    query += f" WHERE {filter_col} > '{last_date}'"
                else:
                    logger.info(f" {table}: Carga Completa (Sin historial).")

                # 2. EXTRACCIÓN
                df = pd.read_sql(query, self.engine_prod)
                if df.empty:
                    logger.info(f" {table}: Sin novedades.")
                    continue

                # 3. ENMASCARAMIENTO
                for col, rule in table_conf.get('masking_rules', {}).items():
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: self.mask_value(x, rule))

                # 4. CARGA (UPSERT SIMULADO)
                with self.engine_qa.connect() as conn:
                    # Borramos los IDs que vamos a insertar para evitar duplicados
                    if not df.empty:
                        ids = tuple(df[pk].tolist())
                        if ids:
                            id_str = str(ids) if len(ids) > 1 else f"({ids[0]})"
                            conn.execute(text(f"DELETE FROM {table} WHERE {pk} IN {id_str}"))
                            conn.commit()
                
                df.to_sql(table, self.engine_qa, if_exists='append', index=False, method='multi')
                self.log_audit(table, len(df), "SUCCESS - INCREMENTAL")
                logger.info(f" {table}: {len(df)} nuevos registros sincronizados.")

            except Exception as e:
                logger.error(f" Error {table}: {e}")
                self.log_audit(table, 0, "ERROR", str(e))

if __name__ == "__main__":
    ETLEngine().run_pipeline()