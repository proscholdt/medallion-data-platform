# ==========================================================
# Script: Conversão de Pessoas (Pipedrive JSON -> Parquet)
# ==========================================================
# Este script lê arquivos JSON de pessoas exportadas do Pipedrive,
# armazenados na camada Bronze (Azure Blob Storage), extrai os campos:
# 'id', 'name', 'email' e 'phone', consolida os dados em um único
# DataFrame Polars e salva como arquivo Parquet na camada Silver.
#
# Fluxo:
# - Conexão aos containers Bronze e Silver via Azure Blob.
# - Leitura de todos os JSONs do diretório especificado.
# - Extração segura dos dados principais, pegando o 1º email/telefone.
# - Conversão para Parquet e upload no destino com nome dinâmico.
#
# Objetivo:
# Estruturar e otimizar os dados de pessoas para análises futuras,
# consolidando os arquivos dispersos em um único Parquet padronizado.
# ==========================================================



import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import polars as pl
from io import BytesIO
from datetime import datetime
import json

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
SOURCE_CONTAINER = "bronze"
DEST_CONTAINER = "silver"
SOURCE_FOLDER = "source_pipedrive/person_all"
DEST_FOLDER = "source_pipedrive/person_all"

# Gerar nome dinâmico para o arquivo Parquet
now = datetime.now()
data_atual = now.strftime("%Y%m%d")
hora_atual = now.strftime("%H%M%S")
OUTPUT_FILE_NAME = f"person_all_{data_atual}_{hora_atual}.parquet"

# Conectar ao Azure
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
source_container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
dest_container_client = blob_service_client.get_container_client(DEST_CONTAINER)

# Listar arquivos JSON no diretório de origem
blobs = source_container_client.list_blobs(name_starts_with=SOURCE_FOLDER)
json_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

print(f"🔎 {len(json_files)} arquivos JSON encontrados.")

# Lista para acumular dados simples
registros = []

# Ler e processar todos os arquivos JSON
for json_file in json_files:
    blob_client = source_container_client.get_blob_client(json_file)
    blob_data = blob_client.download_blob().readall()
    pessoa = json.loads(blob_data)

    # Extrair campos desejados com segurança
    registro = {
        "id": pessoa.get("id"),
        "name": pessoa.get("name"),
        "email": None,
        "phone": None
    }

    # Extrair email se existir
    emails = pessoa.get("email", [])
    if isinstance(emails, list) and emails:
        registro["email"] = emails[0].get("value")

    # Extrair phone se existir
    phones = pessoa.get("phone", [])
    if isinstance(phones, list) and phones:
        registro["phone"] = phones[0].get("value")

    registros.append(registro)

# Criar DataFrame com tipagem forçada
df = pl.DataFrame(registros).with_columns([
    pl.col("id").cast(pl.Int64),
    pl.col("name").cast(pl.Utf8),
    pl.col("email").cast(pl.Utf8),
    pl.col("phone").cast(pl.Utf8)
])

# Salvar como Parquet em memória
output_buffer = BytesIO()
df.write_parquet(output_buffer)
output_buffer.seek(0)

# Upload para a pasta de destino com nome dinâmico
dest_blob_client = dest_container_client.get_blob_client(f"{DEST_FOLDER}/{OUTPUT_FILE_NAME}")
dest_blob_client.upload_blob(output_buffer, overwrite=True)

print(f"✅ Arquivo Parquet salvo com sucesso em: {DEST_FOLDER}/{OUTPUT_FILE_NAME}")
