import os
import polars as pl
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from dotenv import load_dotenv

# === Carregar variáveis de ambiente ===
load_dotenv()

# === Conexão com Azure Blob ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"
CAMINHO_SILVER = "source_google/grupos_de_anuncio"
CAMINHO_GOLD = "source_google/dim_grupoAnuncio"
NOME_ARQUIVO_GOLD = "dim_grupoAnuncio.parquet"

# Colunas desejadas (ordem obrigatória)
CAMPOS_DESEJADOS = ["id_grupo", "nome_grupo"]

# === Criar cliente Azure ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

silver_container = blob_service_client.get_container_client(CONTAINER_SILVER)
gold_container = blob_service_client.get_container_client(CONTAINER_GOLD)

# === Ler todos os arquivos Parquet da Silver ===
dfs = []
for blob in silver_container.list_blobs(name_starts_with=CAMINHO_SILVER):
    if not blob.name.endswith(".parquet"):
        continue

    print(f"🔄 Lendo: {blob.name}")
    blob_data = silver_container.download_blob(blob.name).readall()
    df = pl.read_parquet(BytesIO(blob_data))

    # Verifica se todas as colunas desejadas estão presentes
    if all(col in df.columns for col in CAMPOS_DESEJADOS):
        # Reordena conforme CAMPOS_DESEJADOS
        df = df.select(CAMPOS_DESEJADOS)
        dfs.append(df)
    else:
        print(f"⚠️ Ignorado: {blob.name} — colunas incompletas")

# === Consolidar, remover duplicatas e salvar ===
if dfs:
    df_final = pl.concat(dfs, how="vertical").unique()

    # === Salvar no destino da Gold ===
    buffer = BytesIO()
    df_final.write_parquet(buffer)
    buffer.seek(0)

    caminho_destino = f"{CAMINHO_GOLD}/{NOME_ARQUIVO_GOLD}"
    gold_container.upload_blob(name=caminho_destino, data=buffer, overwrite=True)
    print(f"✅ Arquivo salvo em: {caminho_destino}")
else:
    print("⚠️ Nenhum arquivo válido encontrado na Silver.")






# import os
# import polars as pl
# from azure.storage.blob import BlobServiceClient
# from io import BytesIO
# from dotenv import load_dotenv

# # === Carregar variáveis de ambiente ===
# load_dotenv()

# # === Conexão com Azure Blob ===
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

# CONTAINER_SILVER = "silver"
# CONTAINER_GOLD = "gold"
# CAMINHO_SILVER = "source_google/grupos_de_anuncio"
# CAMINHO_GOLD = "source_google/dim_grupoAnuncio"
# NOME_ARQUIVO_GOLD = "dim_grupoAnuncio.parquet"

# # Colunas desejadas
# CAMPOS_DESEJADOS = ["id_grupo", "nome_grupo"]

# # === Criar cliente Azure ===
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# silver_container = blob_service_client.get_container_client(CONTAINER_SILVER)
# gold_container = blob_service_client.get_container_client(CONTAINER_GOLD)

# # === Ler todos os arquivos Parquet da Silver ===
# dfs = []
# for blob in silver_container.list_blobs(name_starts_with=CAMINHO_SILVER):
#     if not blob.name.endswith(".parquet"):
#         continue

#     print(f"🔄 Lendo: {blob.name}")
#     blob_data = silver_container.download_blob(blob.name).readall()
#     df = pl.read_parquet(BytesIO(blob_data))
#     dfs.append(df)

# # === Consolidar, selecionar colunas e remover duplicatas ===
# if dfs:
#     df_final = pl.concat(dfs, how="vertical")
#     df_final = df_final.select([col for col in CAMPOS_DESEJADOS if col in df_final.columns]).unique()

#     # === Salvar no destino da Gold ===
#     buffer = BytesIO()
#     df_final.write_parquet(buffer)
#     buffer.seek(0)

#     caminho_destino = f"{CAMINHO_GOLD}/{NOME_ARQUIVO_GOLD}"
#     gold_container.upload_blob(name=caminho_destino, data=buffer, overwrite=True)
#     print(f"✅ Arquivo salvo em: {caminho_destino}")
# else:
#     print("⚠️ Nenhum arquivo Parquet encontrado na Silver.")
