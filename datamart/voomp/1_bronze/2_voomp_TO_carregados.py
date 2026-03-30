import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import io

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"

# Conexão Azure Blob
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Caminhos
pasta_origem = 'source_voomp/voomp'
pasta_destino = 'source_voomp/voomp_carregados'

# Listar blobs na pasta origem com "+/" para evitar pegar blobs fora da pasta
blobs = container_client.list_blobs(name_starts_with=pasta_origem + '/')

for blob in blobs:
    print(f"🔄 Movendo: {blob.name}")

    # Baixar o blob da origem
    downloader = container_client.download_blob(blob)
    blob_data = io.BytesIO()
    downloader.readinto(blob_data)
    blob_data.seek(0)

    # Definir o novo caminho na pasta destino
    nome_arquivo = os.path.basename(blob.name)
    novo_blob_name = f"{pasta_destino}/{nome_arquivo}"

    # Upload para a pasta destino
    container_client.upload_blob(name=novo_blob_name, data=blob_data, overwrite=True)
    print(f"✅ Blob movido para: {novo_blob_name}")

    # Deletar o blob original após mover
    container_client.delete_blob(blob)
    print(f"🗑️ Blob original excluído: {blob.name}")
