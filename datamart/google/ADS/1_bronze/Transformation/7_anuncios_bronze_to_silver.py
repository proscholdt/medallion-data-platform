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
CAMINHO_BRONZE = "source_google/anuncios"
CAMINHO_SILVER = "source_google/anuncios"

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
colunas_float = ["ctr", "cpc_medio", "custo", "conversoes"]
colunas_int   = ["impressoes", "cliques"]

# Colunas texto (URLs, nomes etc.)
schema_texto = {
    "final_urls": pl.Utf8,
    "youtube_video_id": pl.Utf8,
    "youtube_url": pl.Utf8,
    "thumbnail_url": pl.Utf8,
    "nome_anuncio": pl.Utf8,
    "tipo_anuncio": pl.Utf8,
    "rede": pl.Utf8,
    "dispositivo": pl.Utf8,
    "data": pl.Utf8,  # vamos parsear depois para Date
}

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

    # ✅ Leitura robusta:
    # - infer_schema_length=None: examina o arquivo todo para inferir tipos
    # - schema_overrides: garante que URLs & textos fiquem como string
    df = pl.read_csv(
        StringIO(csv_text),
        infer_schema_length=None,
        schema_overrides=schema_texto,
        ignore_errors=False,
        # Se necessário, ajuste separador/aspas:
        # separator=";", quote_char='"'
    )

    # === CONVERSÃO DE TIPOS ===
    # data
    if "data" in df.columns:
        df = df.with_columns(
            pl.col("data")
              .cast(pl.Utf8)
              .str.strip_chars()
              .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
              .alias("data")
        )

    # floats (tratando vírgula decimal)
    for col in colunas_float:
        if col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(col).is_null())
                  .then(None)
                  .otherwise(pl.col(col).cast(pl.Utf8).str.replace(",", "."))
                  .cast(pl.Float64, strict=False)
                  .fill_null(0.0)
                  .alias(col)
            )

    # inteiros
    for col in colunas_int:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Int64, strict=False).fill_null(0).alias(col)
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

print("🏁 Todos os arquivos de anúncios convertidos e enviados para Silver.")
