# ==========================================================
# Move blobs .parquet de all_stages_and_pipelines → all_stages_and_pipelines_carregados
# Mantém arquivos antigos no destino (não sobrescreve)
# ==========================================================

import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Carregar variáveis de ambiente
load_dotenv()

# Configuração Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "silver"
SOURCE_FOLDER = "source_pipedrive/all_stages_and_pipelines"
DEST_FOLDER = "source_pipedrive/all_stages_and_pipelines_carregados"

# Conectar ao Azure Blob Storage
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Listar blobs .parquet no diretório de origem
blobs = container_client.list_blobs(name_starts_with=SOURCE_FOLDER)
parquet_blobs = [blob.name for blob in blobs if blob.name.endswith(".parquet")]

print(f"🔍 Encontrados {len(parquet_blobs)} arquivos .parquet para mover.")

# Loop para mover e deletar os arquivos
for idx, blob_name in enumerate(parquet_blobs, 1):
    file_name_only = os.path.basename(blob_name)
    dest_blob_name = f"{DEST_FOLDER}/{file_name_only}"

    dest_blob_client = container_client.get_blob_client(dest_blob_name)

    # Se já existir no destino, pula
    if dest_blob_client.exists():
        print(f"⚠️ [{idx}] Arquivo já existe no destino, pulando: {dest_blob_name}")
        continue

    print(f"📦 [{idx}] Movendo {blob_name} → {dest_blob_name}")

    # Baixar blob original
    blob_data = container_client.get_blob_client(blob_name).download_blob().readall()

    # Subir para destino (sem sobrescrever)
    dest_blob_client.upload_blob(blob_data)
    print(f"✅ [{idx}] Enviado para {dest_blob_name}")

    # Deletar original (somente após mover com sucesso)
    container_client.get_blob_client(blob_name).delete_blob()
    print(f"🗑️ [{idx}] Deletado {blob_name}")

print("🎉 Movimentação concluída com sucesso.")
