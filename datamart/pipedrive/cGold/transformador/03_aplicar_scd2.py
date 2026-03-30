import polars as pl
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# === CONFIG ===
load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

CONTAINER_NAME = "gold"
FTMOVE_PATH = "source_pipedrive/ft_move"
DIM_CONTAINER_NAME = "silver"
DIM_PATH = "source_pipedrive/scd2_dim_stage/scd2_dim_stage.parquet"

# === CONECTA AZURE ===
blob_service_client = BlobServiceClient(
    f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
    credential=STORAGE_ACCOUNT_KEY
)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
dim_container_client = blob_service_client.get_container_client(DIM_CONTAINER_NAME)

# === LÊ DIMENSÃO SCD2 VIGENTE ===
print("🔍 Lendo dimensão SCD2 da silver...")
dim_local = "scd2_dim_stage.parquet"
with open(dim_local, "wb") as f:
    f.write(dim_container_client.download_blob(DIM_PATH).readall())

dim = pl.read_parquet(dim_local)
dim_vigente = dim.filter(pl.col("dt_fim_vigencia") == datetime(9999, 12, 31, 23, 59, 59))
dim_vigente = dim_vigente.select(["stage_name", "pipeline_id", "stage_id"]).rename({"stage_id": "stage_id_atual"})
dim_base = dim.select(["stage_id", "stage_name", "pipeline_id"])

# === PROCESSA ARQUIVOS DA FATO ===
print("📁 Corrigindo arquivos da fato...")
blobs = container_client.list_blobs(name_starts_with=FTMOVE_PATH + "/")

for blob in blobs:
    nome_arquivo = os.path.basename(blob.name)
    if not nome_arquivo.endswith(".parquet"):
        continue

    print(f"➡️  Processando {nome_arquivo}...")
    local_file = f"temp_{nome_arquivo}"

    with open(local_file, "wb") as f:
        f.write(container_client.download_blob(blob.name).readall())

    df = pl.read_parquet(local_file)

    # Corrige new_stage_id
    if "new_stage_id" in df.columns:
        df = df.join(dim_base, left_on="new_stage_id", right_on="stage_id", how="left", suffix="_dim_new")
        df = df.join(dim_vigente, left_on=["stage_name_dim_new", "pipeline_id_dim_new"], right_on=["stage_name", "pipeline_id"], how="left")
        df = df.with_columns([
            pl.when(pl.col("stage_id_atual").is_not_null()).then(pl.col("stage_id_atual")).otherwise(pl.col("new_stage_id")).alias("new_stage_id")
        ])

    # Corrige old_stage_id
    if "old_stage_id" in df.columns:
        df = df.join(dim_base, left_on="old_stage_id", right_on="stage_id", how="left", suffix="_dim_old")
        df = df.join(dim_vigente, left_on=["stage_name_dim_old", "pipeline_id_dim_old"], right_on=["stage_name", "pipeline_id"], how="left")
        df = df.with_columns([
            pl.when(pl.col("stage_id_atual").is_not_null()).then(pl.col("stage_id_atual")).otherwise(pl.col("old_stage_id")).alias("old_stage_id")
        ])

    # Remove colunas intermediárias
    colunas_para_remover = [col for col in df.columns if col.startswith("stage_name") or col.startswith("pipeline_id") or col.startswith("stage_id") or col.endswith("_dim_new") or col.endswith("_dim_old")]
    df = df.drop([col for col in colunas_para_remover if col in df.columns])

    # Salva e reenvia
    df.write_parquet(local_file)
    with open(local_file, "rb") as f:
        container_client.upload_blob(blob.name, f, overwrite=True)

    os.remove(local_file)
    print(f"✅ Atualizado: {blob.name}")

os.remove(dim_local)
print("\n🏁 Substituição de new_stage_id e old_stage_id concluída com sucesso.")
