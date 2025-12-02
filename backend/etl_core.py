import os
import yaml
import hashlib
import logging
import uuid
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from faker import Faker
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

# Logs estructurados en consola también
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLEngine:
    def __init__(self):
        config_path = os.path.join(BASE_DIR, 'config.yaml')
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.faker = Faker('es_MX')
        self.salt = os.getenv("HASH_SALT", "default_salt").encode()
        
        # ID Único de Ejecución (Para rastrear todo este lote de trabajo)
        self.execution_id = str(uuid.uuid4())
        
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

    def log_audit(self, data):
        """Escribe el log detallado en QA (Req 5)"""
        try:
            with self.engine_qa.connect() as conn:
                conn.execute(text("""
                    INSERT INTO auditoria (
                        id_ejecucion, tabla, operacion, registros_procesados, 
                        reglas_aplicadas, fecha_inicio, fecha_fin, estado, mensaje
                    ) VALUES (:eid, :tb, :op, :rec, :rules, :start, :end, :st, :msg)
                """), {
                    "eid": self.execution_id,
                    "tb": data['tabla'],
                    "op": data['operacion'],
                    "rec": data['registros'],
                    "rules": json.dumps(data['reglas']), # Guardamos reglas como JSON string
                    "start": data['inicio'],
                    "end": datetime.now(),
                    "st": data['estado'],
                    "msg": str(data.get('error', 'OK'))[:200]
                })
                conn.commit()
        except Exception as e:
            logger.error(f" Fallo escribiendo auditoría: {e}")

    def get_max_date(self, table, col):
        try:
            with self.engine_qa.connect() as conn:
                return conn.execute(text(f"SELECT MAX({col}) FROM {table}")).scalar()
        except Exception: return None

    def run_pipeline(self):
        logger.info(f" Iniciando Pipeline (ID: {self.execution_id})")
        
        for table_conf in self.config['tables']:
            table = table_conf['name']
            pk = table_conf['pk']
            filter_col = table_conf.get('filter_column')
            rules = table_conf.get('masking_rules', {})
            
            # Capturar hora de inicio por tabla
            start_time = datetime.now()
            
            logger.info(f" Analizando: {table}")
            
            try:
                # 1. DETECTAR DELTA
                last_date = self.get_max_date(table, filter_col)
                query = f"SELECT * FROM {table}"
                operacion = "FULL"
                
                if last_date and filter_col:
                    query += f" WHERE {filter_col} > '{last_date}'"
                    operacion = "INCREMENTAL"
                
                # 2. EXTRACCIÓN
                df = pd.read_sql(query, self.engine_prod)
                
                if df.empty:
                    logger.info(f"    Sin novedades.")
                    continue

                # 3. ENMASCARAMIENTO
                for col, rule in rules.items():
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: self.mask_value(x, rule))

                # 4. CARGA (UPSERT)
                with self.engine_qa.connect() as conn:
                    if not df.empty:
                        ids = tuple(df[pk].tolist())
                        if ids:
                            id_str = str(ids) if len(ids) > 1 else f"({ids[0]})"
                            conn.execute(text(f"DELETE FROM {table} WHERE {pk} IN {id_str}"))
                            conn.commit()
                
                df.to_sql(table, self.engine_qa, if_exists='append', index=False, method='multi')
                
                # 5. AUDITORÍA COMPLETA
                self.log_audit({
                    "tabla": table,
                    "operacion": operacion,
                    "registros": len(df),
                    "reglas": rules,
                    "inicio": start_time,
                    "estado": "SUCCESS"
                })
                logger.info(f" {table}: {len(df)} registros ({operacion}).")

            except Exception as e:
                logger.error(f" Error {table}: {e}")
                self.log_audit({
                    "tabla": table,
                    "operacion": "ERROR",
                    "registros": 0,
                    "reglas": rules,
                    "inicio": start_time,
                    "estado": "ERROR",
                    "error": str(e)
                })

if __name__ == "__main__":
    ETLEngine().run_pipeline()