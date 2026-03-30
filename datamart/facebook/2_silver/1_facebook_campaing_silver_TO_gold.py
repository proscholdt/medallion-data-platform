
# import os
# from dotenv import load_dotenv
# from azure.storage.blob import BlobServiceClient

# # Carrega variáveis de ambiente
# load_dotenv()
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

# # Containers e caminhos
# SILVER_CONTAINER = "silver"
# GOLD_CONTAINER = "gold"
# # Caminho exato onde estão os arquivos na camada Silver (sem barra no final)
# SILVER_FOLDER = "source_facebook/facebook_camp"
# # Mesmo caminho para salvar em Gold (sem alteração)
# GOLD_FOLDER = "source_facebook/facebook_camp"

# # Conexão com o Azure
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service = BlobServiceClient.from_connection_string(connection_string)
# silver_client = blob_service.get_container_client(SILVER_CONTAINER)
# gold_client = blob_service.get_container_client(GOLD_CONTAINER)

# # Listar blobs em todo o container Silver e filtrar somente os arquivos diretamente na pasta "source_facebook/facebook_camp"
# print("🔍 Buscando arquivos no Silver...")
# blobs = []
# for blob in silver_client.list_blobs():
#     # Verificar se o blob está exatamente na pasta desejada e tem extensão .parquet
#     if os.path.dirname(blob.name) == SILVER_FOLDER and blob.name.endswith(".parquet"):
#         blobs.append(blob.name)

# if not blobs:
#     print("⚠️ Nenhum arquivo Parquet encontrado na Silver na pasta exata.")
#     exit()

# # Mover arquivos um a um
# for blob_name in blobs:
#     file_name = os.path.basename(blob_name)
#     destino_blob = f"{GOLD_FOLDER}/{file_name}"
#     print(f"🔁 Movendo {blob_name} → {destino_blob}")

#     # Obter a URL de origem do blob
#     source_url = f"{silver_client.url}/{blob_name}"
#     dest_blob_client = gold_client.get_blob_client(destino_blob)

#     # Iniciar a cópia do blob para o container GOLD
#     copy = dest_blob_client.start_copy_from_url(source_url)



# import os
# import polars as pl
# from dotenv import load_dotenv
# from azure.storage.blob import BlobServiceClient
# from io import BytesIO

# # Carrega variáveis de ambiente
# load_dotenv()
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

# # Containers e caminhos
# SILVER_CONTAINER = "silver"
# GOLD_CONTAINER = "gold"
# SILVER_FOLDER = "source_facebook/facebook_camp"
# GOLD_FOLDER = "source_facebook/facebook_camp"

# # Conexão com o Azure
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service = BlobServiceClient.from_connection_string(connection_string)
# silver_client = blob_service.get_container_client(SILVER_CONTAINER)
# gold_client = blob_service.get_container_client(GOLD_CONTAINER)

# # Mapeamento: de colunas sem sufixo para com sufixo
# MAPEAMENTO_COLUNAS = {
#     "id": "campaign_id",
#     "name": "campaign_name",
#     "objective": "campaign_objective",
#     "status": "campaign_status"
# }

# # Função para transformar Parquet
# def transformar_parquet(parquet_bytes):
#     df = pl.read_parquet(BytesIO(parquet_bytes))

#     colunas_para_renomear = {col: novo for col, novo in MAPEAMENTO_COLUNAS.items() if col in df.columns}

#     if colunas_para_renomear:
#         print(f"➡️ Adicionando sufixo às colunas: {colunas_para_renomear}")
#         df = df.rename(colunas_para_renomear)
#     else:
#         print("⚠️ Nenhuma coluna para renomear encontrada.")

#     buffer = BytesIO()
#     df.write_parquet(buffer)
#     buffer.seek(0)
#     return buffer

# # Listar blobs em todo o container Silver
# print("🔍 Buscando arquivos no Silver...")
# blobs = []
# for blob in silver_client.list_blobs():
#     if os.path.dirname(blob.name) == SILVER_FOLDER and blob.name.endswith(".parquet"):
#         blobs.append(blob.name)

# if not blobs:
#     print("⚠️ Nenhum arquivo Parquet encontrado na Silver na pasta exata.")
#     exit()

# # Processar e mover arquivos
# for blob_name in blobs:
#     file_name = os.path.basename(blob_name)
#     destino_blob = f"{GOLD_FOLDER}/{file_name}"
#     print(f"🔁 Processando e movendo {blob_name} → {destino_blob}")

#     # Baixar o arquivo do Silver
#     silver_blob_client = silver_client.get_blob_client(blob_name)
#     download_stream = silver_blob_client.download_blob()
#     parquet_bytes = download_stream.readall()

#     # Transformar o Parquet
#     parquet_transformado = transformar_parquet(parquet_bytes)

#     # Upload do arquivo transformado para o Gold
#     dest_blob_client = gold_client.get_blob_client(destino_blob)
#     dest_blob_client.upload_blob(parquet_transformado, overwrite=True)
#     print(f"✅ Arquivo transformado e movido para: {destino_blob}")

# print("🏁 Processo finalizado com sucesso.")




import os
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import polars as pl
from dotenv import load_dotenv

# ================================
# Variáveis de ambiente
# ================================
print("⚙️  Carregando variáveis de ambiente...")
load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"
SILVER_FOLDER = "source_facebook/facebook_camp"
GOLD_FOLDER = "source_facebook/facebook_camp"

# ================================
# Conexão Azure Blob
# ================================
print("☁️  Conectando ao Azure Blob Storage...")
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
silver_client = blob_service_client.get_container_client(CONTAINER_SILVER)
gold_client = blob_service_client.get_container_client(CONTAINER_GOLD)

# ================================
# Processar arquivos Parquet da Silver para a Gold
# ================================
print("🔄 Iniciando migração de arquivos Parquet da Silver para Gold...")

prefix = f"{SILVER_FOLDER}/"
blobs = silver_client.list_blobs(name_starts_with=prefix)

for blob in blobs:
    if not blob.name.startswith(prefix) or "/" in blob.name[len(prefix):]:
        continue
    if not blob.name.endswith(".parquet"):
        continue

    print(f"📄 Lendo {blob.name}...")
    blob_data = silver_client.download_blob(blob.name).readall()

    # Leitura do Parquet como Polars
    try:
        df = pl.read_parquet(BytesIO(blob_data))
    except Exception as e:
        print(f"❌ Erro ao ler {blob.name}: {e}")
        continue

    # Nome de destino na Gold
    filename = os.path.basename(blob.name)
    gold_path = f"{GOLD_FOLDER}/{filename}"

    # Salvar em buffer
    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    # Upload para Gold
    print(f"⬆️  Enviando {gold_path} para Gold...")
    gold_client.upload_blob(gold_path, buffer, overwrite=True)

print("✅ Migração concluída.")
