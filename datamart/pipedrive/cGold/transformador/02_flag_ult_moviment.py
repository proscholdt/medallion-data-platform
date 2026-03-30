
import os
from azure.storage.blob import BlobServiceClient
import polars as pl
from io import BytesIO
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
GOLD_CONTAINER = "gold"
GOLD_FOLDER = "source_pipedrive/ft_move"
BACKUP_FOLDER = "source_pipedrive/backup"

# Conexão com o Azure Blob Storage
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(GOLD_CONTAINER)

# Listar arquivos Parquet
blobs = container_client.list_blobs(name_starts_with=GOLD_FOLDER)
parquet_files = [blob.name for blob in blobs if blob.name.endswith(".parquet")]

print(f"🔍 {len(parquet_files)} arquivos Parquet encontrados na Gold.")

# Passo 1: Backup antes de modificar qualquer coisa (via download/upload)
for file_path in parquet_files:
    file_name = os.path.basename(file_path)
    backup_path = f"{BACKUP_FOLDER}/{file_name}"

    source_blob = container_client.get_blob_client(file_path)
    dest_blob = container_client.get_blob_client(backup_path)

    # Baixar o conteúdo
    stream = BytesIO()
    source_blob.download_blob().readinto(stream)
    stream.seek(0)

    # Subir para o destino (backup)
    dest_blob.upload_blob(stream, overwrite=True)
    print(f"🗂 Backup criado: {backup_path}")

print("✅ Backup concluído para todos os arquivos.\n")

# Passo 2: Carregar dados para cálculo global
dfs = []
all_columns = set()

for file_path in parquet_files:
    blob_client = container_client.get_blob_client(file_path)
    stream = BytesIO()
    blob_data = blob_client.download_blob()
    blob_data.readinto(stream)
    stream.seek(0)

    df = pl.read_parquet(stream)
    df = df.with_columns(pl.lit(file_path).alias("source_file"))

    dfs.append(df)
    all_columns.update(df.columns)

all_columns = list(all_columns)

# Passo 3: Alinhar colunas em todos os DataFrames
aligned_dfs = []
for df in dfs:
    missing_cols = set(all_columns) - set(df.columns)
    for col in missing_cols:
        df = df.with_columns(pl.lit(None).alias(col))
    df = df.select(all_columns)
    aligned_dfs.append(df)

# Passo 4: Concatenar e calcular último movimento global
df_all = pl.concat(aligned_dfs)

df_all = df_all.with_columns(
    (pl.col("timestamp") == pl.col("timestamp").max().over("deal_id"))
    .cast(pl.Int64)
    .alias("flg_ultimo_movimento")
)

# Passo 5: Separar por arquivo e sobrescrever no blob
for file_path in parquet_files:
    df_file = df_all.filter(pl.col("source_file") == file_path).drop("source_file")

    out_stream = BytesIO()
    df_file.write_parquet(out_stream)
    out_stream.seek(0)

    blob_client = container_client.get_blob_client(file_path)
    blob_client.upload_blob(out_stream, overwrite=True)
    print(f"✅ Arquivo atualizado e sobrescrito: {file_path}")

print("🏁 Atualização concluída para todos os arquivos Gold.")





