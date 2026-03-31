

# ==========================================================
# ETL: Silver → Gold (deal_move → ft_move)
# Une todos Parquet válidos, transforma e exporta único arquivo (sem fk_allstages)
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
SILVER_FOLDER = "source_pipedrive/deal_move"
GOLD_FOLDER = "source_pipedrive/ft_move"

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

# Tipos alvo
int_cols = [
    "deal_id", "pipeline_id", "old_stage_id", "new_stage_id",
    "person_id", "deal_ativo"
]
double_cols = ["value"]
datetime_cols = ["timestamp"]

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
        print(f"⚠️ Ignorado (não parquet): {blob.name}")
        continue
    if blob.size < 100:
        print(f"⚠️ Ignorado (arquivo pequeno/vazio): {blob.name}")
        continue

    print(f"📥 Lendo {blob.name}")
    downloader = silver_client.download_blob(blob.name)
    data = downloader.readall()
    try:
        df = pl.read_parquet(BytesIO(data))
        dfs.append(df)
        print(f"✅ Lido com sucesso: {blob.name} (linhas: {df.height}, colunas: {df.width})")
    except Exception as e:
        print(f"❌ Erro ao ler {blob.name}: {e}")
        continue

if not dfs:
    print("⚠️ Nenhum arquivo válido encontrado. Encerrando.")
    exit()

# Unir todos os DataFrames
df_all = pl.concat(dfs, how="vertical_relaxed")

# Checagem pós-concatenação
print("\n📊 Após concatenação:")
print(f"timestamp dtype antes de transformar: {df_all.schema['timestamp']}")
print(f"timestamp nulos: {df_all.select(pl.col('timestamp').is_null().sum()).item()} / {df_all.height}")

# Remover owner_id se existir
if "owner_id" in df_all.columns:
    df_all = df_all.drop("owner_id")

# Remover fk_allstages se existir
if "fk_allstages" in df_all.columns:
    df_all = df_all.drop("fk_allstages")
    print("✅ Coluna fk_allstages removida.")

# Preencher nulos de texto com ""
text_cols = [col for col, dtype in zip(df_all.columns, df_all.dtypes) if dtype == pl.Utf8]
for col in text_cols:
    df_all = df_all.with_columns(pl.col(col).fill_null("").alias(col))

# Conversão de tipos
for col in int_cols:
    if col in df_all.columns:
        df_all = df_all.with_columns(pl.col(col).cast(pl.Int64, strict=False))
for col in double_cols:
    if col in df_all.columns:
        df_all = df_all.with_columns(pl.col(col).cast(pl.Float64, strict=False))

for col in datetime_cols:
    if col in df_all.columns:
        col_dtype = df_all.schema[col]
        if col_dtype == pl.Utf8:
            # Limpeza e padronização prévia
            df_all = df_all.with_columns(
                pl.col(col)
                .str.slice(0, 19)                     # pega só primeiros 19 caracteres
                .str.replace_all("T", " ")            # troca T por espaço
                .alias(col)
            )
            # Conversão robusta para datetime
            df_all = df_all.with_columns(
                pl.col(col).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
            )
            print(f"✅ Coluna {col} cortada e convertida para Datetime.")
        elif col_dtype == pl.Datetime:
            print(f"⏩ Coluna {col} já é Datetime, mantendo como está.")
        else:
            print(f"⚠️ Coluna {col} tem tipo inesperado ({col_dtype}), não convertida.")

# Renomear deal_ativo → flag_deal_ativo
if "deal_ativo" in df_all.columns:
    df_all = df_all.rename({"deal_ativo": "flag_deal_ativo"})

# Criar coluna date (apenas data do timestamp)
df_all = df_all.with_columns(pl.col("timestamp").dt.date().alias("date"))

# Criar coluna flg_ultimo_movimento (marcando apenas 1 por deal_id)
df_all = df_all.sort(["deal_id", "timestamp"], descending=[False, True])
df_all = df_all.with_columns([
    (pl.col("timestamp") == pl.col("timestamp").max().over("deal_id"))
    .cast(pl.Int64)
    .alias("flg_ultimo_movimento")
])

# Nome do arquivo final
now = datetime.now()
data_atual = now.strftime("%Y%m%d")
hora_atual = now.strftime("%H%M%S")
output_file = f"fat_mov_{data_atual}_{hora_atual}.parquet"
gold_blob_path = f"{GOLD_FOLDER}/{output_file}"

# Exportar Parquet final para Gold
out_buffer = BytesIO()
df_all.write_parquet(out_buffer)
out_buffer.seek(0)
gold_client.upload_blob(name=gold_blob_path, data=out_buffer, overwrite=True)

print(f"\n✅ Arquivo final gravado na Gold: {gold_blob_path}")
