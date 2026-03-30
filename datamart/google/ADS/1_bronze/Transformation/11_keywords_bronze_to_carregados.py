from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

# === CARREGAR VARIÁVEIS DE AMBIENTE ===
load_dotenv()

# === CONFIGURAÇÕES AZURE ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
PASTA_ORIGEM = "source_google/keywords"
PASTA_DESTINO = "source_google/carregados/keywords_c"

# === CONEXÃO COM BLOB STORAGE ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# === LISTA E MOVE OS ARQUIVOS ===
blobs = container_client.list_blobs(name_starts_with=PASTA_ORIGEM + "/")

for blob in blobs:
    origem = blob.name
    nome_arquivo = origem.split("/")[-1]
    destino = f"{PASTA_DESTINO}/{nome_arquivo}"

    print(f"🔁 Movendo: {origem} → {destino}")

    # Copiar conteúdo para o novo caminho
    origem_url = container_client.get_blob_client(origem).url
    destino_blob = container_client.get_blob_client(destino)
    destino_blob.start_copy_from_url(origem_url)

    # Remover blob original após cópia
    container_client.delete_blob(origem)

    print(f"✅ Movido com sucesso: {nome_arquivo}")

print("🏁 Todos os arquivos foram movidos para a pasta 'carregados'.")
