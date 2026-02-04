Creación de ambiente

python -m venv .venv

.venv\Scripts\activate.bat (terminal)
& .\.venv\Scripts\Activate.ps1 (powershell)

Uso

# Instalar dependencias
pip install -r requirements.txt

# Copiar y configurar variables de entorno
cp .env.example .env

# Ejecutar pipeline API → PostgreSQL
python main.py api-to-postgres --endpoint /products

# Ejecutar pipeline PostgreSQL → Qdrant (embeddings)
python main.py postgres-to-qdrant

# Sincronización completa
python main.py full-sync

# Forzar regeneración de embeddings
python main.py postgres-to-qdrant --force


Cuando conozcas la estructura de tu API, puedes adaptar el mapeo en api_extractor.py y el modelo en product.py