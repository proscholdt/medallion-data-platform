import polars as pl
from azure.storage.blob import BlobServiceClient
from io import BytesIO, StringIO
from dotenv import load_dotenv
import os

# === CARREGAR VARIÁVEIS DE AMBIENTE ===
load_dotenv()

# === VARIÁVEIS DE CONEXÃO ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_BRONZE = "bronze"
CONTAINER_SILVER = "silver"
CAMINHO_BRONZE = "source_google/grupos_de_anuncio"
CAMINHO_SILVER = "source_google/grupos_de_anuncio"

# === CONEXÃO COM AZURE BLOB STORAGE ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
bronze_container = blob_service_client.get_container_client(CONTAINER_BRONZE)
silver_container = blob_service_client.get_container_client(CONTAINER_SILVER)

# === DEFINIÇÃO DE COLUNAS POR TIPO ===
colunas_float = [
    "ctr", "cpc_medio", "custo", "conversoes"
]

colunas_int = [
    "impressoes", "cliques"
]

# === LISTAR E PROCESSAR CSVs DO BRONZE ===
blobs = bronze_container.list_blobs(name_starts_with=CAMINHO_BRONZE + "/")

for blob in blobs:
    blob_name = blob.name
    if not blob_name.endswith(".csv"):
        continue

    print(f"🔄 Processando: {blob_name}")

    blob_client = bronze_container.get_blob_client(blob_name)
    csv_bytes = blob_client.download_blob().readall()
    csv_text = csv_bytes.decode("utf-8")
    df = pl.read_csv(StringIO(csv_text))

    # === CONVERSÃO DE TIPOS ===
    for col in df.columns:
        if col in colunas_float:
            df = df.with_columns(
                pl.col(col)
                .cast(str)
                .str.replace(",", ".", literal=False)
                .cast(pl.Float64, strict=False)
                .fill_null(0.0)
                .alias(col)
            )

        elif col in colunas_int:
            df = df.with_columns(
                pl.col(col)
                .cast(pl.Int64, strict=False)
                .fill_null(0)
                .alias(col)
            )

        elif col == "data":
            df = df.with_columns(
                pl.col("data")
                .cast(str)
                .str.strip_chars()
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias("data")
            )

    # === CONVERTE PARA PARQUET EM MEMÓRIA ===
    parquet_buffer = BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)

    # === ENVIO PARA SILVER ===
    nome_arquivo_parquet = blob_name.split("/")[-1].replace(".csv", ".parquet")
    caminho_silver = f"{CAMINHO_SILVER}/{nome_arquivo_parquet}"

    silver_blob = silver_container.get_blob_client(caminho_silver)
    silver_blob.upload_blob(parquet_buffer.getvalue(), overwrite=True)

    print(f"✅ Enviado para Silver: {caminho_silver}")

print("🏁 Todos os arquivos de grupos convertidos e enviados para Silver.")
