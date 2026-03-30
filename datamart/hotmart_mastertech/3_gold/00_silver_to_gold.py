import os
import io
import polars as pl
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from dotenv import load_dotenv

# === ENV ===
load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env")

# === CONFIG ===
CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"
DIRECTORY = "sourcer_mastertech"  # prefix

# Nomes dos arquivos na camada silver
ARQUIVO_SALES_SILVER = "hotmart_sales.parquet"
ARQUIVO_USERS_SILVER = "hotmart_users.parquet"

# Nomes dos arquivos na camada gold (saída)
ARQUIVO_SALES_GOLD = "hotmart_sales.parquet"
ARQUIVO_USERS_GOLD = "hotmart_users.parquet"

connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
service = BlobServiceClient.from_connection_string(connection_string)


def download_blob_bytes(container: str, blob_name: str) -> bytes:
    bc = service.get_blob_client(container=container, blob=blob_name)
    return bc.download_blob().readall()


def upload_bytes(container: str, blob_name: str, data: bytes) -> None:
    bc = service.get_blob_client(container=container, blob=blob_name)
    bc.upload_blob(data, overwrite=True)


prefix = DIRECTORY.strip("/").replace("\\", "/") + "/"

# === 1) DEFINIR CAMINHOS DOS ARQUIVOS NA SILVER ===
blob_sales_in = f"{prefix}{ARQUIVO_SALES_SILVER}"
blob_users_in = f"{prefix}{ARQUIVO_USERS_SILVER}"

print(f"Usando blob SALES de entrada: {CONTAINER_SILVER}/{blob_sales_in}")
print(f"Usando blob USERS de entrada: {CONTAINER_SILVER}/{blob_users_in}")

# === 2) BAIXAR E LER PARQUETS (SALES E USERS) ===
try:
    parquet_sales_bytes = download_blob_bytes(CONTAINER_SILVER, blob_sales_in)
    parquet_users_bytes = download_blob_bytes(CONTAINER_SILVER, blob_users_in)
except ResourceNotFoundError:
    print("Blob não encontrado. Confira o caminho acima.")
    raise

df_sales = pl.read_parquet(io.BytesIO(parquet_sales_bytes))
df_users = pl.read_parquet(io.BytesIO(parquet_users_bytes))

# === 3) TRATAR DATAS APENAS EM SALES ===
# garante approved_datetime_utc
if "approved_datetime_utc" not in df_sales.columns:
    if "approved_date" not in df_sales.columns:
        raise ValueError("Não achei 'approved_datetime_utc' nem 'approved_date' no parquet de SALES.")
    df_sales = df_sales.with_columns(
        pl.from_epoch("approved_date", time_unit="ms").alias("approved_datetime_utc")
    )

# cria BR (UTC-3 fixo) + string BR
df_sales = df_sales.with_columns(
    (pl.col("approved_datetime_utc") - pl.duration(hours=3)).alias("approved_datetime_brt"),
    (pl.col("approved_datetime_utc") - pl.duration(hours=3))
        .dt.strftime("%d/%m/%Y %H:%M:%S")
        .alias("approved_datetime_br")
)

# === 4) ESCREVER EM MEMÓRIA E SUBIR PARA GOLD (DOIS ARQUIVOS SEPARADOS) ===

# SALES -> GOLD (pasta 'vendas')
out_sales_buffer = io.BytesIO()
df_sales.write_parquet(out_sales_buffer)
out_sales_buffer.seek(0)

blob_sales_out = f"{prefix}vendas/{ARQUIVO_SALES_GOLD}"
upload_bytes(CONTAINER_GOLD, blob_sales_out, out_sales_buffer.getvalue())

print(f"OK: SALES enviado para {CONTAINER_GOLD}/{blob_sales_out}")
print("Colunas adicionadas em SALES: approved_datetime_brt (datetime), approved_datetime_br (string BR)")

# USERS -> GOLD (pasta 'usuarios', sem alterações de data)
out_users_buffer = io.BytesIO()
df_users.write_parquet(out_users_buffer)
out_users_buffer.seek(0)

blob_users_out = f"{prefix}usuarios/{ARQUIVO_USERS_GOLD}"
upload_bytes(CONTAINER_GOLD, blob_users_out, out_users_buffer.getvalue())

print(f"OK: USERS enviado para {CONTAINER_GOLD}/{blob_users_out}")
