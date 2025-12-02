import os
import yaml
import hashlib
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from faker import Faker
from dotenv import load_dotenv

# Configuración de rutas y logs
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

# Configuración de Logs (Req 5: Auditoría y logs)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLEngine:
    def __init__(self):
        # Cargar configuración (Req 1: Configuración paramétrica)
        config_path = os.path.join(BASE_DIR, 'config.yaml')
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.faker = Faker('es_MX')
        # Semilla para Hash Determinístico (Req 2)
        self.salt = os.getenv("HASH_SALT", "default_salt").encode()
        
        # Conexiones a los 2 Servidores (Req 8: Seguridad - Credenciales en .env)
        try:
            prod_uri = os.getenv(self.config['databases']['source_db_env_var'])
            qa_uri = os.getenv(self.config['databases']['target_db_env_var'])
            
            # pool_pre_ping ayuda a mantener vivas las conexiones en la nube
            self.engine_prod = create_engine(prod_uri, pool_pre_ping=True)
            self.engine_qa = create_engine(qa_uri, pool_pre_ping=True)
            logger.info(" Conexión establecida con ambos servidores Supabase")
        except Exception as e:
            logger.error(f" Error de conexión: {e}")
            raise

    # --- FUNCIONES DE ENMASCARAMIENTO (Req 2) ---
    def mask_value(self, value, rule):
        if value is None: return None
        
        if rule == 'hash_email':
            # Pseudo-anonimización determinística: SHA256(valor + salt)
            hash_object = hashlib.sha256(str(value).encode() + self.salt)
            return f"{hash_object.hexdigest()[:10]}@anon.com"
            
        elif rule == 'fake_name':
            # Sustitución sintética
            return self.faker.name()
            
        elif rule == 'preserve_format':
            # Preservar formatos (Simulación de mantener estructura +52)
            # En un caso real usaríamos regex complejo, aquí regeneramos con faker manteniendo prefijo
            return f"+52 ({self.faker.random_int(55,99)}) {self.faker.random_int(100,999)}-{self.faker.random_int(1000,9999)}"
            
        elif rule == 'redact':
            # Redacción
            return "****"
            
        return value # 'none' pasa el valor original

    def log_audit(self, table, records, status, error=None):
        """Escribe en la tabla 'auditoria' de QA (Req 5)"""
        try:
            with self.engine_qa.connect() as conn:
                conn.execute(text("""
                    INSERT INTO auditoria (tabla, registros_procesados, estado, mensaje, fecha_ejecucion)
                    VALUES (:t, :r, :s, :m, :f)
                """), {
                    "t": table, "r": records, "s": status, 
                    "m": str(error)[:200] if error else "OK", "f": datetime.now()
                })
                conn.commit()
        except Exception as e:
            logger.error(f" Fallo al escribir auditoría en BD: {e}")

    def run_pipeline(self):
        logger.info(" Iniciando Pipeline ETL...")
        
        # Leemos la configuración de días (con default a 90 si no existe)
        dias_historia = self.config['settings'].get('extraction_window_days', 90)
        logger.info(f" Ventana de extracción configurada: Últimos {dias_historia} días")

        for table_conf in self.config['tables']:
            table_name = table_conf['name']
            filter_col = table_conf.get('filter_column') # Ej: "fecha_registro"
            
            logger.info(f" Procesando tabla: {table_name}")
            
            try:
                # 1. EXTRACCIÓN CON FILTRO (Req 1 y 4)
                # Construimos la query dinámica
                if filter_col:
                    # Sintaxis PostgreSQL: WHERE fecha >= NOW() - INTERVAL '90 days'
                    query = f"""
                        SELECT * FROM {table_name} 
                        WHERE {filter_col} >= NOW() - INTERVAL '{dias_historia} days'
                    """
                else:
                    # Si no tiene columna de fecha, traemos todo (fallback)
                    query = f"SELECT * FROM {table_name}"
                
                # Ejecutar lectura
                df = pd.read_sql(query, self.engine_prod)
                
                if df.empty:
                    logger.warning(f" No hay registros nuevos en {table_name} (últimos {dias_historia} días).")
                    continue

                # ... (El resto del código: Enmascaramiento y Carga sigue IGUAL) ...
                
                # 2. TRANSFORMACIÓN / ENMASCARAMIENTO
                rules = table_conf.get('masking_rules', {})
                for col, rule in rules.items():
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: self.mask_value(x, rule))

                # 3. CARGA
                with self.engine_qa.connect() as conn:
                    # IMPORTANTE: Para simular carga incremental en la demo, 
                    # podrías quitar el TRUNCATE si quieres acumular, 
                    # pero para pruebas limpias lo dejamos o usamos idempotencia real.
                    # conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE")) # Opcional según estrategia
                    pass

                df.to_sql(table_name, self.engine_qa, if_exists='append', index=False, method='multi', chunksize=500)
                
                self.log_audit(table_name, len(df), "SUCCESS")
                logger.info(f" {len(df)} registros migrados exitosamente.")

            except Exception as e:
                logger.error(f" Error crítico en tabla {table_name}: {e}")
                self.log_audit(table_name, 0, "ERROR", str(e))

if __name__ == "__main__":
    # Ejecutar el proceso
    engine = ETLEngine()
    engine.run_pipeline()