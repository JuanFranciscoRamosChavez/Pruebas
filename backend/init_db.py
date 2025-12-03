import os
import yaml
import random
from faker import Faker
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Cargar configuración
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))
with open(os.path.join(BASE_DIR, 'config.yaml'), 'r') as f:
    config = yaml.safe_load(f)

fake = Faker('es_MX')

def init_tables():
    prod_uri = os.getenv(config['databases']['source_db_env_var'])
    qa_uri = os.getenv(config['databases']['target_db_env_var'])
    
    if not prod_uri or not qa_uri:
        print(" Error: Faltan URIs")
        return

    engine_prod = create_engine(prod_uri)
    engine_qa = create_engine(qa_uri)
    
    # ---------------------------------------------------------
    # 1. PREPARAR PRODUCCIÓN (CON INTEGRIDAD ENTRE PRODUCTOS)
    # ---------------------------------------------------------
    print(f" Conectando a Producción...")
    with engine_prod.connect() as conn:
        # Borramos todo en orden inverso a las dependencias
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
            -- Creamos Inventario ANTES que Detalles
            CREATE TABLE inventario (
                id SERIAL PRIMARY KEY,
                producto VARCHAR(100) UNIQUE, -- Debe ser único para ser FK
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
                -- CONEXIÓN REALIZADA: FK hacia inventario
                producto VARCHAR(100) REFERENCES inventario(producto), 
                cantidad INTEGER,
                precio_unitario DECIMAL(10, 2)
            );
        """))
        
        print(" Sembrando datos en Producción...")
        
        # 1. Generar Lista de Productos Reales (Inventario)
        lista_productos = []
        for _ in range(30): # 30 productos en el catálogo
            prod = f"{fake.word().capitalize()} {fake.word()}-{random.randint(100,999)}"
            # Evitar duplicados al generar
            while prod in lista_productos:
                prod = f"{fake.word().capitalize()} {fake.word()}-{random.randint(100,999)}"
            lista_productos.append(prod)
            
            conn.execute(text("INSERT INTO inventario (producto, stock, ubicacion) VALUES (:p, :s, :u)"),
                         {"p": prod, "s": random.randint(0, 500), "u": f"Pasillo {random.randint(1,10)}"})

        # 2. Clientes
        for _ in range(50): 
            conn.execute(text("INSERT INTO clientes (nombre, email, telefono, direccion) VALUES (:n, :e, :t, :d)"), 
                         {"n": fake.name(), "e": fake.email(), "t": f"+52 55{random.randint(10000000,99999999)}", "d": fake.address()})
        
        # 3. Órdenes
        for _ in range(100): 
            conn.execute(text("INSERT INTO ordenes (cliente_id, total) VALUES (:c, :t)"), 
                         {"c": random.randint(1, 50), "t": random.uniform(100, 5000)})
        
        # 4. Detalles (Usando SOLO productos que existen en inventario)
        for _ in range(300):
            conn.execute(text("INSERT INTO detalle_ordenes (orden_id, producto, cantidad, precio_unitario) VALUES (:o, :p, :c, :pu)"), 
                         {
                             "o": random.randint(1, 100), 
                             "p": random.choice(lista_productos), # <--- AQUÍ ESTÁ LA CONEXIÓN
                             "c": random.randint(1, 10), 
                             "pu": random.uniform(10, 500)
                         })
        conn.commit()

    # ---------------------------------------------------------
    # 2. PREPARAR QA (ESQUEMA ESPEJO)
    # ---------------------------------------------------------
    print(f" Conectando a QA...")
    with engine_qa.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS detalle_ordenes, ordenes, inventario, clientes, auditoria CASCADE"))
        conn.execute(text("""
            CREATE TABLE clientes (id INTEGER PRIMARY KEY, nombre VARCHAR(100), email VARCHAR(200), telefono VARCHAR(50), direccion VARCHAR(200), fecha_registro TIMESTAMP);
            
            CREATE TABLE inventario (
                id INTEGER PRIMARY KEY, 
                producto VARCHAR(100) UNIQUE, -- Mantenemos la restricción UNIQUE
                stock INTEGER, 
                ubicacion VARCHAR(50), 
                fecha_registro TIMESTAMP
            );
            
            CREATE TABLE ordenes (id INTEGER PRIMARY KEY, cliente_id INTEGER REFERENCES clientes(id), total DECIMAL(10, 2), fecha TIMESTAMP);
            
            CREATE TABLE detalle_ordenes (
                id INTEGER PRIMARY KEY, 
                orden_id INTEGER REFERENCES ordenes(id), 
                -- INTEGRIDAD REFERENCIAL EN QA TAMBIÉN
                producto VARCHAR(100) REFERENCES inventario(producto), 
                cantidad INTEGER, 
                precio_unitario DECIMAL(10, 2)
            );

            CREATE TABLE auditoria (
                id SERIAL PRIMARY KEY, fecha_ejecucion TIMESTAMP, tabla VARCHAR(50), 
                registros_procesados INTEGER, estado VARCHAR(100), mensaje TEXT, 
                id_ejecucion VARCHAR(50), operacion VARCHAR(20), reglas_aplicadas TEXT, 
                fecha_inicio TIMESTAMP, fecha_fin TIMESTAMP
            );
        """))
        conn.commit()
    
    print(" ¡Base de datos regenerada")

if __name__ == "__main__":
    init_tables()