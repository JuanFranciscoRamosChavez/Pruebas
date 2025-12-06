import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# 1. Cargar entorno y clave
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

key = os.getenv("BACKUP_ENCRYPTION_KEY")

if not key:
    print(" ERROR: No se encontró BACKUP_ENCRYPTION_KEY en el archivo .env")
    exit()

# ==========================================
# CONFIGURACIÓN: PON AQUÍ EL NOMBRE DE TU ARCHIVO
# (Copia el nombre del archivo que se generó en la carpeta backups/)
BACKUP_FILENAME = "backup_20251205_210045.sql.enc" 
# ==========================================

input_path = os.path.join(BASE_DIR, 'backups', BACKUP_FILENAME)
output_path = os.path.join(BASE_DIR, 'backups', "restored_script.sql")

def decrypt_backup():
    if not os.path.exists(input_path):
        print(f" El archivo no existe: {input_path}")
        return

    try:
        print(f" Leyendo archivo cifrado: {BACKUP_FILENAME}...")
        with open(input_path, 'rb') as f:
            encrypted_data = f.read()

        print(" Desencriptando datos...")
        fernet = Fernet(key.encode())
        decrypted_data = fernet.decrypt(encrypted_data)

        print(f" Guardando SQL legible en: {output_path}...")
        with open(output_path, 'wb') as f:
            f.write(decrypted_data)

        print("\n ¡ÉXITO! El archivo 'restored_script.sql' está listo.")
        print("  ADVERTENCIA: Este archivo contiene datos reales. Bórralo después de cargarlo en pgAdmin.")

    except Exception as e:
        print(f" Error al desencriptar (¿Es la clave correcta?): {e}")

if __name__ == "__main__":
    decrypt_backup()