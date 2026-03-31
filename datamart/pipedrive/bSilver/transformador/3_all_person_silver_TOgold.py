

# ==========================================================
# ETL: Silver → Gold (person_all → dim_person)
# Une todos Parquet válidos e exporta único arquivo Parquet
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
SILVER_FOLDER = "source_pipedrive/person_all"
GOLD_FOLDER = "source_pipedrive/dim_person"

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

# Coletar DataFrames
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

# Transformar campo id → INT (deixa os outros como estão)
if "id" in df_all.columns:
    df_all = df_all.with_columns(pl.col("id").cast(pl.Int64, strict=False))
    print("✅ Campo 'id' transformado para INT.")

# Nome do arquivo final
now = datetime.now()
data_atual = now.strftime("%Y%m%d")
hora_atual = now.strftime("%H%M%S")
output_file = f"dim_person_{data_atual}_{hora_atual}.parquet"
gold_blob_path = f"{GOLD_FOLDER}/{output_file}"

# Exportar Parquet final para Gold
out_buffer = BytesIO()
df_all.write_parquet(out_buffer)
out_buffer.seek(0)
gold_client.upload_blob(name=gold_blob_path, data=out_buffer, overwrite=True)

print(f"✅ Arquivo final gravado na Gold: {gold_blob_path}")

