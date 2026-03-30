import polars as pl
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import os
from dotenv import load_dotenv
import time

# INICIAR CONTADOR DE TEMPO
inicio = time.time()

# === CARREGAR VARIÁVEIS DE AMBIENTE ===
load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

# === CONFIGURAÇÕES DE CONTAINER E CAMINHOS ===
CONTAINER_BRONZE = "bronze"
CONTAINER_GOLD = "gold"
CAMINHO_ARQUIVO_BRONZE = "source_calls/calls/calls.csv"
CAMINHO_ARQUIVO_GOLD = "source_calls/calls/calls.parquet"

# === CONEXÃO COM AZURE BLOB STORAGE ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# === BAIXAR CSV DA BRONZE ===
bronze_client = blob_service_client.get_container_client(CONTAINER_BRONZE)
blob_csv = bronze_client.download_blob(CAMINHO_ARQUIVO_BRONZE)
csv_bytes = blob_csv.readall()

# === LER COMO DATAFRAME POLARS ===
df = pl.read_csv(BytesIO(csv_bytes))

# === AJUSTAR TIPAGEM DAS COLUNAS ===
df = df.with_columns([
    pl.col("DATA").str.strptime(pl.Date, format="%d/%m/%Y", strict=False),
    pl.col("AGENDAMENTOS").cast(pl.Int64, strict=False),
    pl.col("CALLS REALIZADAS").cast(pl.Int64, strict=False)
])

# === SALVAR COMO PARQUET NA MEMÓRIA ===
buffer_parquet = BytesIO()
df.write_parquet(buffer_parquet)
buffer_parquet.seek(0)

# === ENVIAR PARA GOLD ===
gold_client = blob_service_client.get_container_client(CONTAINER_GOLD)
gold_client.upload_blob(
    name=CAMINHO_ARQUIVO_GOLD,
    data=buffer_parquet,
    overwrite=True
)

# === EXIBIR RESULTADO ===
fim = time.time()
print(f"✅ Arquivo convertido e enviado para GOLD: {CAMINHO_ARQUIVO_GOLD}")
print(f"⏱️ Tempo total de processamento: {fim - inicio:.2f} segundos")
