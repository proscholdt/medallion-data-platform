# ==========================================================
# Script: Mover e Deletar Arquivos JSON (Pipedrive Pessoas)
# ==========================================================
# Este script move todos os arquivos JSON da pasta 'person_all' 
# (camada Bronze) para a pasta 'carregados_person_all' no mesmo 
# container do Azure Blob Storage. Após mover os arquivos, ele 
# chama uma API externa (Abico) para deletar a pasta de origem, 
# garantindo a limpeza do diretório após o processamento.
#
# Fluxo:
# - Conecta ao Blob Storage (usando .env para variáveis).
# - Lista todos os arquivos JSON na pasta de origem.
# - Para cada arquivo: baixa o conteúdo e faz upload para o destino.
# - Após mover tudo, faz uma chamada DELETE para remover a pasta original.
#
# Utilidade:
# Organiza e limpa dados processados, separando arquivos já tratados 
# e evitando retrabalho em execuções futuras.
# ==========================================================


import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import requests

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"

SOURCE_FOLDER = "source_pipedrive/person_all"
DEST_FOLDER = "source_pipedrive/carregados_person_all"

print("🔄 Iniciando conexão com o Azure Blob Storage...")

# Conexão Azure
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

print("✅ Conexão estabelecida com sucesso.\n")

# Listar blobs JSON na pasta de origem
print(f"🔎 Listando arquivos JSON na pasta de origem: {SOURCE_FOLDER}")

blobs = container_client.list_blobs(name_starts_with=SOURCE_FOLDER)
json_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

if not json_files:
    print("⚠️ Nenhum arquivo JSON encontrado na pasta de origem.")
    exit()

print(f"✅ {len(json_files)} arquivos encontrados para mover.\n")

# Processar cada arquivo
for i, blob_name in enumerate(json_files, 1):
    print(f"➡️ [{i}/{len(json_files)}] Processando arquivo: {blob_name}")
    source_blob_client = container_client.get_blob_client(blob_name)
    
    print(f"   📥 Baixando o arquivo...")
    data = source_blob_client.download_blob().readall()

    # Construir o caminho de destino
    file_name_only = os.path.basename(blob_name)
    dest_blob_name = f"{DEST_FOLDER}/{file_name_only}"
    print(f"   📤 Fazendo upload para: {dest_blob_name}")

    # Upload para o destino
    dest_blob_client = container_client.get_blob_client(dest_blob_name)
    dest_blob_client.upload_blob(data, overwrite=True)

    print(f"✅ Arquivo movido com sucesso: {blob_name} ➔ {dest_blob_name}\n")

print("🏁 Todos os arquivos foram movidos com sucesso.\n")

# Após mover, deletar a pasta usando a API da Abico
print("🧹 Chamando a API da Abico para deletar a pasta de origem...\n")

# Endpoint e parâmetros
url = "https://app-orion-dev.azurewebsites.net/api/azure-datalake/delete"
params = {
    "storage_account_name": STORAGE_ACCOUNT_NAME,
    "file_system_name": CONTAINER_NAME,
    "directory_path": SOURCE_FOLDER
}

# API Key do .env
API_KEY = os.getenv("X_API_KEY")
if not API_KEY:
    raise ValueError("A variável de ambiente 'X_API_KEY' não foi encontrada no .env")

# Cabeçalhos com a API Key
headers = {
    "accept": "application/json",
    "X-API-Key": API_KEY
}

print(f"🚀 Enviando requisição DELETE para limpar: {SOURCE_FOLDER}\n")

# Requisição DELETE
response = requests.delete(url, headers=headers, params=params)

# Verificando a resposta
if response.status_code == 200:
    print("✅ Pasta deletada com sucesso pela API!")
    print(response.json())
else:
    print(f"❌ Erro ao deletar a pasta: {response.status_code}")
    print(response.text)

print("\n🎉 Processamento finalizado.")
