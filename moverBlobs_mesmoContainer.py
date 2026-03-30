import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "gold"
SOURCE_FOLDER = "source_pipedrive/ft_move"
DEST_FOLDER = "source_pipedrive/backup"

# Conectar ao Azure Blob Storage
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Listar blobs na pasta de origem
blobs = container_client.list_blobs(name_starts_with=SOURCE_FOLDER)

for blob in blobs:
    blob_name = blob.name
    file_name = os.path.basename(blob_name)
    dest_blob_name = f"{DEST_FOLDER}/{file_name}"

    print(f"🔄 Copiando {blob_name} → {dest_blob_name}")

    # Baixar o blob original
    source_blob_client = container_client.get_blob_client(blob_name)
    data = source_blob_client.download_blob().readall()

    # Enviar para o destino
    dest_blob_client = container_client.get_blob_client(dest_blob_name)
    dest_blob_client.upload_blob(data, overwrite=True)

    print(f"✅ {blob_name} copiado com sucesso.")

print("🎉 Todos os blobs foram copiados com sucesso!")
