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
    # ... (conexión igual que antes) ...
    engine_prod = create_engine(prod_uri)
    engine_qa = create_engine(qa_uri)
    
    # ---------------------------------------------------------
    # 1. PREPARAR PRODUCCIÓN (3 TABLAS)
    # ---------------------------------------------------------
    print(f" Conectando a Producción...")
    with engine_prod.connect() as conn:
        # Borramos en cascada para limpiar todo
        conn.execute(text("DROP TABLE IF EXISTS detalle_ordenes, ordenes, clientes CASCADE"))
        
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
            -- 3ra Tabla Relacionada (Requisito PDF)
            CREATE TABLE detalle_ordenes (
                id SERIAL PRIMARY KEY,
                orden_id INTEGER REFERENCES ordenes(id),
                producto VARCHAR(100),
                cantidad INTEGER,
                precio_unitario DECIMAL(10, 2)
            );
        """))
        
        print(" Sembrando datos en Producción (Nivel 1: Clientes)...")
        for _ in range(50): 
            conn.execute(text(
                "INSERT INTO clientes (nombre, email, telefono, direccion) VALUES (:n, :e, :t, :d)"
            ), {
                "n": fake.name(), 
                "e": fake.email(), 
                "t": f"+52 ({random.randint(55,99)}) {random.randint(1000,9999)}-{random.randint(1000,9999)}", 
                "d": fake.address()
            })
        
        print("🌱 Sembrando datos en Producción (Nivel 2: Órdenes)...")
        for _ in range(100): 
            conn.execute(text("INSERT INTO ordenes (cliente_id, total) VALUES (:c, :t)"), 
                         {"c": random.randint(1, 50), "t": random.uniform(100, 5000)})
                         
        print("🌱 Sembrando datos en Producción (Nivel 3: Detalles)...")
        for _ in range(200): # 2 detalles por orden aprox
            conn.execute(text("""
                INSERT INTO detalle_ordenes (orden_id, producto, cantidad, precio_unitario) 
                VALUES (:o, :p, :c, :pu)
            """), {
                "o": random.randint(1, 100),
                "p": fake.word().capitalize() + " " + fake.word(), # Nombre producto random
                "c": random.randint(1, 10),
                "pu": random.uniform(10, 500)
            })
        
        conn.commit()

    # ---------------------------------------------------------
    # 2. PREPARAR QA (ESQUEMA VACÍO)
    # ---------------------------------------------------------
    print(f" Conectando a QA...")
    with engine_qa.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS detalle_ordenes, ordenes, clientes, auditoria CASCADE"))
        conn.execute(text("""
            CREATE TABLE clientes (
                id INTEGER PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(200), -- Hash
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
                producto VARCHAR(100), -- Aquí aplicaremos máscara
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
    
    print(" ¡Base de datos regenerada con 3 tablas relacionadas!")
    
    print(" ¡Entornos listos! Datos en Producción y Tablas vacías en QA.")

if __name__ == "__main__":
    init_tables()