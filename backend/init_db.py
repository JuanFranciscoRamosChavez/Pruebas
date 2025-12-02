import os
import yaml
import random
from faker import Faker
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Cargar configuración y rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

with open(os.path.join(BASE_DIR, 'config.yaml'), 'r') as f:
    config = yaml.safe_load(f)

fake = Faker('es_MX')

def init_tables():
    # 2. Obtener conexiones seguras desde el .env
    source_env_var = config['databases']['source_db_env_var']
    target_env_var = config['databases']['target_db_env_var']
    
    prod_uri = os.getenv(source_env_var)
    qa_uri = os.getenv(target_env_var)
    
    # Validación de seguridad
    if not prod_uri or not qa_uri:
        print(f" Error: No se encontraron las variables de entorno {source_env_var} o {target_env_var}")
        return

    engine_prod = create_engine(prod_uri)
    engine_qa = create_engine(qa_uri)
    
    # ---------------------------------------------------------
    # 3. PREPARAR PRODUCCIÓN (3 TABLAS CON DATOS)
    # ---------------------------------------------------------
    print(f" Conectando a Producción...")
    with engine_prod.connect() as conn:
        # Borrar tablas viejas para reiniciar limpio
        conn.execute(text("DROP TABLE IF EXISTS detalle_ordenes, ordenes, clientes CASCADE"))
        
        # Crear esquema
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
            CREATE TABLE detalle_ordenes (
                id SERIAL PRIMARY KEY,
                orden_id INTEGER REFERENCES ordenes(id),
                producto VARCHAR(100),
                cantidad INTEGER,
                precio_unitario DECIMAL(10, 2)
            );
        """))
        
        # Generar Datos Falsos (Semilla)
        print(" Sembrando datos en Producción...")
        
        # Clientes
        for _ in range(50): 
            conn.execute(text(
                "INSERT INTO clientes (nombre, email, telefono, direccion) VALUES (:n, :e, :t, :d)"
            ), {
                "n": fake.name(), 
                "e": fake.email(), 
                "t": f"+52 ({random.randint(55,99)}) {random.randint(1000,9999)}-{random.randint(1000,9999)}", 
                "d": fake.address()
            })
        
        # Órdenes
        for _ in range(100): 
            conn.execute(text("INSERT INTO ordenes (cliente_id, total) VALUES (:c, :t)"), 
                         {"c": random.randint(1, 50), "t": random.uniform(100, 5000)})
        
        # Detalles (La 3ra tabla)
        for _ in range(200):
            conn.execute(text("INSERT INTO detalle_ordenes (orden_id, producto, cantidad, precio_unitario) VALUES (:o, :p, :c, :pu)"), 
                         {
                             "o": random.randint(1, 100), 
                             "p": fake.word().capitalize() + " " + fake.word(), 
                             "c": random.randint(1, 10), 
                             "pu": random.uniform(10, 500)
                         })
        
        conn.commit()

    # ---------------------------------------------------------
    # 4. PREPARAR QA (ESQUEMA VACÍO)
    # ---------------------------------------------------------
    print(f" Conectando a QA...")
    with engine_qa.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS detalle_ordenes, ordenes, clientes, auditoria CASCADE"))
        conn.execute(text("""
            CREATE TABLE clientes (
                id INTEGER PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(200),
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
            CREATE TABLE detalle_ordenes (
                id INTEGER PRIMARY KEY,
                orden_id INTEGER,
                producto VARCHAR(100),
                cantidad INTEGER,
                precio_unitario DECIMAL(10, 2)
            );
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
    
    print(" ¡Base de datos inicializada con éxito (3 tablas)!")

if __name__ == "__main__":
    init_tables()