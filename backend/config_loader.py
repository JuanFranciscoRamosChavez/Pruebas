import os
import yaml
from dotenv import load_dotenv

# Cargar variables de entorno del archivo .env
load_dotenv()

class Config:
    def __init__(self, config_path='config.yaml'):
        # Cargar configuración YAML
        with open(config_path, 'r') as file:
            self.settings = yaml.safe_load(file)

    def get_db_uri(self, env_type):
        """Construye la conexión segura usando datos del YAML + Secretos del .env"""
        if env_type == 'source':
            conf = self.settings['source_db']
            user = os.getenv('DB_SOURCE_USER')
            pwd = os.getenv('DB_SOURCE_PASSWORD')
        else:
            conf = self.settings['target_db']
            user = os.getenv('DB_TARGET_USER')
            pwd = os.getenv('DB_TARGET_PASSWORD')
            
        # Retorna string de conexión (ej. para SQLAlchemy)
        return f"postgresql://{user}:{pwd}@{conf['host']}:{conf['port']}/{conf['db_name']}"

# Prueba rápida
if __name__ == "__main__":
    config = Config()
    print(f"Conectando a Source: {config.get_db_uri('source')}")
    print(f"Reglas para Clientes: {config.settings['tables'][0]['masking_rules']}")