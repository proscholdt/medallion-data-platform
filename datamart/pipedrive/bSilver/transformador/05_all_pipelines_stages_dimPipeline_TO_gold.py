# # # # ==========================================================
# # # # Gera dim_pipeline mantendo apenas pipeline_id e pipeline_name únicos
# # # # ==========================================================

# # # import os
# # # from dotenv import load_dotenv
# # # from azure.storage.blob import BlobServiceClient
# # # import polars as pl
# # # from io import BytesIO
# # # from datetime import datetime

# # # # Carregar variáveis de ambiente
# # # load_dotenv()

# # # # Configuração Azure
# # # STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# # # STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# # # SILVER_CONTAINER = "silver"
# # # GOLD_CONTAINER = "gold"
# # # SILVER_FOLDER = "source_pipedrive/all_stages_and_pipelines"
# # # GOLD_FOLDER = "source_pipedrive/dim_pipeline"

# # # # Conectar ao Azure Blob Storage
# # # connection_string = (
# # #     f"DefaultEndpointsProtocol=https;"
# # #     f"AccountName={STORAGE_ACCOUNT_NAME};"
# # #     f"AccountKey={STORAGE_ACCOUNT_KEY};"
# # #     f"EndpointSuffix=core.windows.net"
# # # )
# # # blob_service_client = BlobServiceClient.from_connection_string(connection_string)
# # # silver_client = blob_service_client.get_container_client(SILVER_CONTAINER)
# # # gold_client = blob_service_client.get_container_client(GOLD_CONTAINER)

# # # # Coletar DataFrames
# # # dfs = []
# # # print("🔍 Buscando arquivos Parquet válidos na Silver...")

# # # blobs = silver_client.list_blobs(name_starts_with=SILVER_FOLDER)
# # # for blob in blobs:
# # #     if not blob.name.endswith(".parquet"):
# # #         continue

# # #     print(f"📥 Lendo {blob.name}")
# # #     downloader = silver_client.download_blob(blob.name)
# # #     data = downloader.readall()
# # #     df = pl.read_parquet(BytesIO(data))
# # #     dfs.append(df)

# # # if not dfs:
# # #     print("⚠️ Nenhum arquivo válido encontrado. Encerrando.")
# # #     exit()

# # # # Unir todos os DataFrames
# # # df_all = pl.concat(dfs, how="vertical_relaxed")

# # # # Criar dim_pipeline mantendo apenas campos necessários e eliminando duplicados
# # # dim_pipeline = (
# # #     df_all.select(["pipeline_id", "pipeline_name"])
# # #     .unique()
# # # )

# # # print(f"✅ dim_pipeline montada com {dim_pipeline.height} registros únicos.")

# # # # Nome do arquivo final
# # # now = datetime.now()
# # # data_atual = now.strftime("%Y%m%d")
# # # hora_atual = now.strftime("%H%M%S")
# # # output_file = f"dim_pipeline_{data_atual}_{hora_atual}.parquet"
# # # gold_blob_path = f"{GOLD_FOLDER}/{output_file}"

# # # # Exportar Parquet final para Gold
# # # out_buffer = BytesIO()
# # # dim_pipeline.write_parquet(out_buffer)
# # # out_buffer.seek(0)
# # # gold_client.upload_blob(name=gold_blob_path, data=out_buffer, overwrite=True)

# # # print(f"🎉 Arquivo dim_pipeline gravado na Gold: {gold_blob_path}")





# # # ==========================================================
# # # Gera dim_pipeline mantendo apenas pipeline_id e pipeline_name únicos
# # # ==========================================================

# # import os
# # from dotenv import load_dotenv
# # from azure.storage.blob import BlobServiceClient
# # import polars as pl
# # from io import BytesIO
# # from datetime import datetime

# # # Carregar variáveis de ambiente
# # load_dotenv()

# # # Configuração Azure
# # STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# # STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# # SILVER_CONTAINER = "silver"
# # GOLD_CONTAINER = "gold"
# # SILVER_FOLDER = "source_pipedrive/all_stages_and_pipelines"
# # GOLD_FOLDER = "source_pipedrive/dim_pipeline"

# # # Conectar ao Azure Blob Storage
# # connection_string = (
# #     f"DefaultEndpointsProtocol=https;"
# #     f"AccountName={STORAGE_ACCOUNT_NAME};"
# #     f"AccountKey={STORAGE_ACCOUNT_KEY};"
# #     f"EndpointSuffix=core.windows.net"
# # )
# # blob_service_client = BlobServiceClient.from_connection_string(connection_string)
# # silver_client = blob_service_client.get_container_client(SILVER_CONTAINER)
# # gold_client = blob_service_client.get_container_client(GOLD_CONTAINER)

# # # Excluir blobs .parquet existentes no destino
# # print(f"🗑️ Excluindo arquivos .parquet antigos em {GOLD_FOLDER}...")
# # blobs_to_delete = gold_client.list_blobs(name_starts_with=GOLD_FOLDER)
# # for blob in blobs_to_delete:
# #     if blob.name.endswith(".parquet"):
# #         print(f"➡️ Deletando {blob.name}")
# #         gold_client.delete_blob(blob.name)
# # print("✅ Blobs antigos excluídos.")

# # # Coletar DataFrames da Silver
# # dfs = []
# # print("🔍 Buscando arquivos Parquet válidos na Silver...")
# # blobs = silver_client.list_blobs(name_starts_with=SILVER_FOLDER)
# # for blob in blobs:
# #     if not blob.name.endswith(".parquet"):
# #         continue

# #     print(f"📥 Lendo {blob.name}")
# #     downloader = silver_client.download_blob(blob.name)
# #     data = downloader.readall()
# #     df = pl.read_parquet(BytesIO(data))
# #     dfs.append(df)

# # if not dfs:
# #     print("⚠️ Nenhum arquivo válido encontrado. Encerrando.")
# #     exit()

# # # Unir todos os DataFrames
# # df_all = pl.concat(dfs, how="vertical_relaxed")

# # # Criar dim_pipeline mantendo apenas campos necessários e eliminando duplicados
# # dim_pipeline = (
# #     df_all.select(["pipeline_id", "pipeline_name"])
# #     .unique()
# # )

# # print(f"✅ dim_pipeline montada com {dim_pipeline.height} registros únicos.")

# # # Nome do arquivo final
# # now = datetime.now()
# # data_atual = now.strftime("%Y%m%d")
# # hora_atual = now.strftime("%H%M%S")
# # output_file = f"dim_pipeline_{data_atual}_{hora_atual}.parquet"
# # gold_blob_path = f"{GOLD_FOLDER}/{output_file}"

# # # Exportar Parquet final para Gold
# # out_buffer = BytesIO()
# # dim_pipeline.write_parquet(out_buffer)
# # out_buffer.seek(0)
# # gold_client.upload_blob(name=gold_blob_path, data=out_buffer, overwrite=True)

# # print(f"🎉 Arquivo dim_pipeline gravado na Gold: {gold_blob_path}")




# # ==========================================================
# # Gera dim_pipeline mantendo apenas pipeline_id únicos
# # ==========================================================

# import os
# from dotenv import load_dotenv
# from azure.storage.blob import BlobServiceClient
# import polars as pl
# from io import BytesIO
# from datetime import datetime

# # Carregar variáveis de ambiente
# load_dotenv()

# # Configuração Azure
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# SILVER_CONTAINER = "silver"
# GOLD_CONTAINER = "gold"
# SILVER_FOLDER = "source_pipedrive/all_stages_and_pipelines"
# GOLD_FOLDER = "source_pipedrive/dim_pipeline"

# # Conectar ao Azure Blob Storage
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)
# silver_client = blob_service_client.get_container_client(SILVER_CONTAINER)
# gold_client = blob_service_client.get_container_client(GOLD_CONTAINER)

# # Excluir blobs .parquet existentes no destino
# print(f"🗑️ Excluindo arquivos .parquet antigos em {GOLD_FOLDER}...")
# blobs_to_delete = gold_client.list_blobs(name_starts_with=GOLD_FOLDER)
# for blob in blobs_to_delete:
#     if blob.name.endswith(".parquet"):
#         print(f"➡️ Deletando {blob.name}")
#         gold_client.delete_blob(blob.name)
# print("✅ Blobs antigos excluídos.")

# # Coletar DataFrames da Silver
# dfs = []
# print("🔍 Buscando arquivos Parquet válidos na Silver...")
# blobs = silver_client.list_blobs(name_starts_with=SILVER_FOLDER)
# for blob in blobs:
#     if not blob.name.endswith(".parquet"):
#         continue

#     print(f"📥 Lendo {blob.name}")
#     downloader = silver_client.download_blob(blob.name)
#     data = downloader.readall()
#     df = pl.read_parquet(BytesIO(data))
#     dfs.append(df)

# if not dfs:
#     print("⚠️ Nenhum arquivo válido encontrado. Encerrando.")
#     exit()

# # Unir todos os DataFrames
# df_all = pl.concat(dfs, how="vertical_relaxed")

# # Criar dim_pipeline mantendo apenas pipeline_id únicos
# dim_pipeline = (
#     df_all.select(["pipeline_id", "pipeline_name"])
#     .unique(subset=["pipeline_id"])
# )

# print(f"✅ dim_pipeline montada com {dim_pipeline.height} registros únicos por pipeline_id.")

# # Nome do arquivo final
# now = datetime.now()
# data_atual = now.strftime("%Y%m%d")
# hora_atual = now.strftime("%H%M%S")
# output_file = f"dim_pipeline_{data_atual}_{hora_atual}.parquet"
# gold_blob_path = f"{GOLD_FOLDER}/{output_file}"

# # Exportar Parquet final para Gold
# out_buffer = BytesIO()
# dim_pipeline.write_parquet(out_buffer)
# out_buffer.seek(0)
# gold_client.upload_blob(name=gold_blob_path, data=out_buffer, overwrite=True)

# print(f"🎉 Arquivo dim_pipeline gravado na Gold: {gold_blob_path}")





# ==========================================================
# Gera dim_pipeline mantendo apenas pipeline_id únicos
# ==========================================================

import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import polars as pl
from io import BytesIO
from datetime import datetime

# Carregar variáveis de ambiente
load_dotenv()

# Configuração Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
SILVER_CONTAINER = "silver"
GOLD_CONTAINER = "gold"
SILVER_FOLDER = "source_pipedrive/all_stages_and_pipelines"
GOLD_FOLDER = "source_pipedrive/dim_pipeline"

# Conectar ao Azure Blob Storage
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
silver_client = blob_service_client.get_container_client(SILVER_CONTAINER)
gold_client = blob_service_client.get_container_client(GOLD_CONTAINER)

# Excluir blobs .parquet existentes no destino
print(f"🗑️ Excluindo arquivos .parquet antigos em {GOLD_FOLDER}...")
blobs_to_delete = gold_client.list_blobs(name_starts_with=GOLD_FOLDER)
for blob in blobs_to_delete:
    if blob.name.endswith(".parquet"):
        print(f"➡️ Deletando {blob.name}")
        gold_client.delete_blob(blob.name)
print("✅ Blobs antigos excluídos.")

# Coletar DataFrames da Silver
dfs = []
print("🔍 Buscando arquivos Parquet válidos na Silver...")
blobs = silver_client.list_blobs(name_starts_with=SILVER_FOLDER)
for blob in blobs:
    # Pega o caminho relativo depois do prefixo base
    relative_path = blob.name[len(SILVER_FOLDER):].lstrip("/")

    # Ignora se contiver subpastas
    if "/" in relative_path:
        print(f"⚠️ Ignorado (subpasta): {blob.name}")
        continue

    if not blob.name.endswith(".parquet"):
        continue

    print(f"📥 Lendo {blob.name}")
    downloader = silver_client.download_blob(blob.name)
    data = downloader.readall()
    df = pl.read_parquet(BytesIO(data))
    dfs.append(df)

if not dfs:
    print("⚠️ Nenhum arquivo válido encontrado. Encerrando.")
    exit()

# Unir todos os DataFrames
df_all = pl.concat(dfs, how="vertical_relaxed")

# Criar dim_pipeline mantendo apenas pipeline_id únicos
dim_pipeline = (
    df_all.select(["pipeline_id", "pipeline_name"])
    .unique(subset=["pipeline_id"])
)

print(f"✅ dim_pipeline montada com {dim_pipeline.height} registros únicos por pipeline_id.")

# Nome do arquivo final
now = datetime.now()
data_atual = now.strftime("%Y%m%d")
hora_atual = now.strftime("%H%M%S")
output_file = f"dim_pipeline_{data_atual}_{hora_atual}.parquet"
gold_blob_path = f"{GOLD_FOLDER}/{output_file}"

# Exportar Parquet final para Gold
out_buffer = BytesIO()
dim_pipeline.write_parquet(out_buffer)
out_buffer.seek(0)
gold_client.upload_blob(name=gold_blob_path, data=out_buffer, overwrite=True)

print(f"🎉 Arquivo dim_pipeline gravado na Gold: {gold_blob_path}")

