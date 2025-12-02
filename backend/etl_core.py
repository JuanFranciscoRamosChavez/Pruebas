import os
import yaml
import hashlib
import logging
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from faker import Faker
from dotenv import load_dotenv

# Configuración
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLEngine:
    def __init__(self):
        # Configuración básica
        with open(os.path.join(BASE_DIR, 'config.yaml'), 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.faker = Faker('es_MX')
        self.salt = os.getenv("HASH_SALT", "default_salt").encode()
        self.max_retries = 3  # Requerimiento 6: Intentar hasta N veces
        self.retry_wait = 2   # Segundos de espera (constante, no exponencial)

        try:
            prod_uri = os.getenv(self.config['databases']['source_db_env_var'])
            qa_uri = os.getenv(self.config['databases']['target_db_env_var'])
            self.engine_prod = create_engine(prod_uri, pool_pre_ping=True)
            self.engine_qa = create_engine(qa_uri, pool_pre_ping=True)
        except Exception as e:
            logger.error(f" Error crítico de conexión inicial: {e}")
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
                # Nos aseguramos de cortar el mensaje de error para que quepa en TEXT
                msg = str(error)[:500] if error else "OK"
                conn.execute(text("""
                    INSERT INTO auditoria (tabla, registros_procesados, estado, mensaje, fecha_ejecucion)
                    VALUES (:t, :r, :s, :m, :f)
                """), {
                    "t": table, 
                    "r": records, 
                    "s": status, 
                    "m": msg, 
                    "f": datetime.now()
                })
                conn.commit()
        except Exception as e:
            logger.error(f" Fallo al escribir en auditoría (posible caída de QA): {e}")

    def process_table(self, table_conf):
        """Lógica de procesamiento de una sola tabla con reintentos"""
        table_name = table_conf['name']
        filter_col = table_conf.get('filter_column')
        dias = self.config['settings'].get('extraction_window_days', 90)
        
        # --- BUCLE DE REINTENTOS (REQ 6) ---
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f" Procesando {table_name} (Intento {attempt}/{self.max_retries})...")
                
                # 1. Extracción
                query = f"SELECT * FROM {table_name}"
                if filter_col:
                    query += f" WHERE {filter_col} >= NOW() - INTERVAL '{dias} days'"
                
                df = pd.read_sql(query, self.engine_prod)
                
                if df.empty:
                    logger.info(f"    Sin datos recientes en {table_name}.")
                    return # Terminamos con éxito (sin hacer nada)

                # 2. Transformación
                for col, rule in table_conf.get('masking_rules', {}).items():
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: self.mask_value(x, rule))

                # 3. Carga
                with self.engine_qa.connect() as conn:
                    # Limpieza (Carga Completa simulada para integridad)
                    conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                    conn.commit()
                
                df.to_sql(table_name, self.engine_qa, if_exists='append', index=False, method='multi', chunksize=500)
                
                # Éxito: Logueamos y salimos del bucle de reintentos
                self.log_audit(table_name, len(df), "SUCCESS")
                logger.info(f" {len(df)} registros migrados en {table_name}.")
                return 

            except Exception as e:
                logger.error(f" Fallo en intento {attempt}: {e}")
                
                if attempt == self.max_retries:
                    # Si fue el último intento, registramos el ERROR definitivo
                    logger.error(f" Se agotaron los reintentos para {table_name}.")
                    self.log_audit(table_name, 0, "ERROR", f"Max retries exceeded: {e}")
                else:
                    # Si quedan intentos, esperamos un poco
                    logger.info(f" Reintentando en {self.retry_wait} segundos...")
                    time.sleep(self.retry_wait)

    def run_pipeline(self):
        logger.info(" Iniciando Pipeline con Tolerancia a Fallos...")
        for table_conf in self.config['tables']:
            self.process_table(table_conf)

if __name__ == "__main__":
    ETLEngine().run_pipeline()