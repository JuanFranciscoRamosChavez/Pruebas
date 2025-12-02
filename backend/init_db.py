import os
import yaml
import random
from faker import Faker
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Cargar configuración segura
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))
with open(os.path.join(BASE_DIR, 'config.yaml'), 'r') as f:
    config = yaml.safe_load(f)

fake = Faker('es_MX')

def init_tables():
    # Obtener conexiones de las variables de entorno
    prod_uri = os.getenv(config['databases']['source_db_env_var'])
    qa_uri = os.getenv(config['databases']['target_db_env_var'])
    
    if not prod_uri or not qa_uri:
        print(" Error: Faltan las URIs en el archivo .env")
        return

    engine_prod = create_engine(prod_uri)
    engine_qa = create_engine(qa_uri)
    
    # ---------------------------------------------------------
    # 1. PREPARAR SERVIDOR DE PRODUCCIÓN
    # ---------------------------------------------------------
    print(f" Conectando a Producción...")
    with engine_prod.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ordenes, clientes CASCADE"))
        conn.execute(text("""
            CREATE TABLE clientes (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(100),
                telefono VARCHAR(50),
                direccion VARCHAR(200),
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE ordenes (
                id SERIAL PRIMARY KEY,
                cliente_id INTEGER REFERENCES clientes(id),
                total DECIMAL(10, 2),
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        
        print(" Sembrando datos falsos en Producción...")
        # Crear 50 clientes
        for _ in range(50): 
            conn.execute(text(
                "INSERT INTO clientes (nombre, email, telefono, direccion) VALUES (:n, :e, :t, :d)"
            ), {
                "n": fake.name(), 
                "e": fake.email(), 
                # Formato fijo para probar regla "preserve_format"
                "t": f"+52 ({random.randint(55,99)}) {random.randint(1000,9999)}-{random.randint(1000,9999)}", 
                "d": fake.address()
            })
        
        # Crear órdenes asociadas
        for _ in range(100): 
            conn.execute(text("INSERT INTO ordenes (cliente_id, total) VALUES (:c, :t)"), 
                         {"c": random.randint(1, 50), "t": random.uniform(100, 5000)})
        conn.commit()

    # ---------------------------------------------------------
    # 2. PREPARAR SERVIDOR DE QA
    # ---------------------------------------------------------
    print(f" Conectando a QA...")
    with engine_qa.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS ordenes, clientes, auditoria CASCADE"))
        conn.execute(text("""
            -- Tablas destino (Sin AUTO_INCREMENT en ID para mantener integridad con Prod)
            CREATE TABLE clientes (
                id INTEGER PRIMARY KEY, 
                nombre VARCHAR(100),
                email VARCHAR(200), -- Hash es largo
                telefono VARCHAR(50),
                direccion VARCHAR(200),
                fecha_registro TIMESTAMP
            );
            CREATE TABLE ordenes (
                id INTEGER PRIMARY KEY,
                cliente_id INTEGER,
                total DECIMAL(10, 2),
                fecha TIMESTAMP
            );
            -- Tabla de Auditoría (Requerimiento 5)
            CREATE TABLE auditoria (
                id SERIAL PRIMARY KEY,
                fecha_ejecucion TIMESTAMP,
                tabla VARCHAR(50),
                registros_procesados INTEGER,
                estado VARCHAR(20),
                mensaje TEXT
            );
        """))
        conn.commit()
    
    print(" ¡Entornos listos! Datos en Producción y Tablas vacías en QA.")

if __name__ == "__main__":
    init_tables()