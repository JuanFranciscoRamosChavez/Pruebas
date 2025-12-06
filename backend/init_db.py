import os
import yaml
import random
from faker import Faker
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Cargar configuración con UTF-8
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

with open(os.path.join(BASE_DIR, 'config.yaml'), 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

fake = Faker('es_MX')

def generate_source_data(counts=None):
    """
    Regenera el esquema y siembra datos en Producción y QA.
    """
    if not counts:
        counts = { "productos": 30, "clientes": 50, "ordenes": 100, "detalles": 300 }

    prod_uri = os.getenv(config['databases']['source_db_env_var'])
    qa_uri = os.getenv(config['databases']['target_db_env_var'])
    
    if not prod_uri or not qa_uri:
        raise Exception("Faltan las URIs de base de datos en .env")

    engine_prod = create_engine(prod_uri)
    engine_qa = create_engine(qa_uri)
    
    # --- 1. PRODUCCIÓN (RECREAR TODO) ---
    print(f" Conectando a Producción...")
    with engine_prod.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS detalle_ordenes, ordenes, inventario, clientes CASCADE"))
        
        conn.execute(text("""
            CREATE TABLE clientes (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(100),
                telefono VARCHAR(50),
                direccion VARCHAR(200),
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE inventario (
                id SERIAL PRIMARY KEY,
                producto VARCHAR(100) UNIQUE,
                stock INTEGER,
                ubicacion VARCHAR(50),
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
                producto VARCHAR(100) REFERENCES inventario(producto), 
                cantidad INTEGER,
                precio_unitario DECIMAL(10, 2)
            );
        """))
        
        print(f" Sembrando datos en Producción...")
        
        # Generar Datos
        limit_prod = int(counts.get('productos', 30))
        lista_productos = []
        for _ in range(limit_prod): 
            prod = f"{fake.word().capitalize()} {fake.word()}-{random.randint(100,999)}"
            while prod in lista_productos: prod = f"{fake.word().capitalize()} {fake.word()}-{random.randint(100,999)}"
            lista_productos.append(prod)
            conn.execute(text("INSERT INTO inventario (producto, stock, ubicacion) VALUES (:p, :s, :u)"), {"p": prod, "s": random.randint(0, 500), "u": f"Pasillo {random.randint(1,10)}"})

        limit_cli = int(counts.get('clientes', 50))
        for _ in range(limit_cli): 
            conn.execute(text("INSERT INTO clientes (nombre, email, telefono, direccion) VALUES (:n, :e, :t, :d)"), {"n": fake.name(), "e": fake.email(), "t": f"+52 55{random.randint(10000000,99999999)}", "d": fake.address()})
        
        limit_ord = int(counts.get('ordenes', 100))
        for _ in range(limit_ord): 
            conn.execute(text("INSERT INTO ordenes (cliente_id, total) VALUES (:c, :t)"), {"c": random.randint(1, limit_cli), "t": random.uniform(100, 5000)})
        
        limit_det = int(counts.get('detalles', 300))
        for _ in range(limit_det):
            conn.execute(text("INSERT INTO detalle_ordenes (orden_id, producto, cantidad, precio_unitario) VALUES (:o, :p, :c, :pu)"), {"o": random.randint(1, limit_ord), "p": random.choice(lista_productos), "c": random.randint(1, 10), "pu": random.uniform(10, 500)})
        conn.commit()

    # --- 2. QA (MANTENER HISTORIAL, ACTUALIZAR ESTRUCTURA) ---
    print(f" Preparando esquema limpio en QA...")
    with engine_qa.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS detalle_ordenes, ordenes, inventario, clientes CASCADE"))
        
        conn.execute(text("""
            CREATE TABLE clientes (id INTEGER PRIMARY KEY, nombre VARCHAR(100), email VARCHAR(200), telefono VARCHAR(50), direccion VARCHAR(200), fecha_registro TIMESTAMP);
            CREATE TABLE inventario (id INTEGER PRIMARY KEY, producto VARCHAR(100) UNIQUE, stock INTEGER, ubicacion VARCHAR(50), fecha_registro TIMESTAMP);
            CREATE TABLE ordenes (id INTEGER PRIMARY KEY, cliente_id INTEGER REFERENCES clientes(id), total DECIMAL(10, 2), fecha TIMESTAMP);
            CREATE TABLE detalle_ordenes (id INTEGER PRIMARY KEY, orden_id INTEGER REFERENCES ordenes(id), producto VARCHAR(100) REFERENCES inventario(producto), cantidad INTEGER, precio_unitario DECIMAL(10, 2));

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
            
            -- ESTAS LÍNEAS AGREGAN LAS COLUMNAS FALTANTES SI LA TABLA YA EXISTÍA
            ALTER TABLE auditoria ADD COLUMN IF NOT EXISTS fecha_inicio TIMESTAMP;
            ALTER TABLE auditoria ADD COLUMN IF NOT EXISTS fecha_fin TIMESTAMP;
        """))
        conn.commit()
    
    print(" ¡Base de datos regenerada y estructura de auditoría actualizada!")

if __name__ == "__main__":
    generate_source_data()