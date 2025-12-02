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

# Configuración de rutas y entorno
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLEngine:
    def __init__(self):
        # PUNTO 1: Configuración Paramétrica
        config_path = os.path.join(BASE_DIR, 'config.yaml')
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.faker = Faker('es_MX')
        self.salt = os.getenv("HASH_SALT", "default_salt").encode()
        
        # Configuración de Reintentos (PUNTO 6)
        self.max_retries = 3  
        self.retry_wait = 2   
        
        # PUNTO 8: Seguridad (Credenciales ocultas)
        try:
            prod_uri = os.getenv(self.config['databases']['source_db_env_var'])
            qa_uri = os.getenv(self.config['databases']['target_db_env_var'])
            self.engine_prod = create_engine(prod_uri, pool_pre_ping=True)
            self.engine_qa = create_engine(qa_uri, pool_pre_ping=True)
        except Exception as e:
            logger.error(f" Error crítico de configuración: {e}")
            raise

    # PUNTO 2: Enmascaramiento
    def mask_value(self, value, rule):
        if value is None: return None
        if rule == 'hash_email':
            return hashlib.sha256(str(value).encode() + self.salt).hexdigest()[:12] + "@anon.com"
        elif rule == 'fake_name': return self.faker.name()
        elif rule == 'preserve_format': return f"+52 ({self.faker.random_int(55,99)}) ***-****"
        elif rule == 'redact': return "****"
        return value

    # PUNTO 5: Auditoría
    def log_audit(self, table, records, status, error=None):
        try:
            with self.engine_qa.connect() as conn:
                conn.execute(text("""
                    INSERT INTO auditoria (tabla, registros_procesados, estado, mensaje, fecha_ejecucion)
                    VALUES (:t, :r, :s, :m, :f)
                """), {
                    "t": table, 
                    "r": records, 
                    "s": status, 
                    "m": str(error)[:500] if error else "OK", 
                    "f": datetime.now()
                })
                conn.commit()
        except Exception as e:
            logger.error(f" Fallo crítico en auditoría: {e}")

    # PUNTO 4: Lógica Incremental (Marca de Agua)
    def get_max_date(self, table, col):
        try:
            with self.engine_qa.connect() as conn:
                return conn.execute(text(f"SELECT MAX({col}) FROM {table}")).scalar()
        except Exception: return None

    # Procesamiento individual por tabla con Reintentos
    def process_table(self, table_conf):
        table = table_conf['name']
        pk = table_conf['pk']
        filter_col = table_conf.get('filter_column')
        
        # Bucle de Reintentos (PUNTO 6)
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f" Procesando {table} (Intento {attempt}/{self.max_retries})...")
                
                # 1. Detectar Delta (PUNTO 4)
                last_date = self.get_max_date(table, filter_col)
                query = f"SELECT * FROM {table}"
                mode = "FULL"
                
                if last_date and filter_col:
                    query += f" WHERE {filter_col} > '{last_date}'"
                    mode = "INCREMENTAL"
                
                # 2. Extracción
                df = pd.read_sql(query, self.engine_prod)
                
                if df.empty:
                    logger.info(f"    {table}: Sin novedades.")
                    return # Éxito (nada que hacer)

                logger.info(f"    {table}: {len(df)} registros nuevos ({mode}).")

                # 3. Transformación (PUNTO 2)
                for col, rule in table_conf.get('masking_rules', {}).items():
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: self.mask_value(x, rule))

                # 4. Carga con Idempotencia (PUNTO 7 - Upsert simulado)
                with self.engine_qa.connect() as conn:
                    if not df.empty:
                        ids = tuple(df[pk].tolist())
                        if ids:
                            # Truco para tupla de 1 elemento en SQL
                            id_str = str(ids) if len(ids) > 1 else f"({ids[0]})"
                            # Borramos lo viejo antes de meter lo nuevo (evita duplicados)
                            conn.execute(text(f"DELETE FROM {table} WHERE {pk} IN {id_str}"))
                            conn.commit()
                
                # Insertar en lotes (PUNTO 9)
                df.to_sql(table, self.engine_qa, if_exists='append', index=False, method='multi', chunksize=500)
                
                # Log final y SALIDA del bucle
                self.log_audit(table, len(df), f"SUCCESS - {mode}")
                logger.info(f" {table}: Sincronización completada.")
                return 

            except Exception as e:
                logger.error(f" Fallo en intento {attempt}: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait)
                else:
                    # Si se acaban los intentos, registramos error final
                    logger.error(f" {table}: Se agotaron los reintentos.")
                    self.log_audit(table, 0, "ERROR", str(e))

    def run_pipeline(self):
        logger.info(" Iniciando Pipeline Maestro...")
        for table_conf in self.config['tables']:
            self.process_table(table_conf)

if __name__ == "__main__":
    ETLEngine().run_pipeline()