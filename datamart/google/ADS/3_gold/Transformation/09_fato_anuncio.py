import os
import polars as pl
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from dotenv import load_dotenv

# === Carregar variáveis de ambiente ===
load_dotenv()

# === Variáveis de conexão e caminhos ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"
CAMINHO_ORIGEM = "source_google/transformados/anuncios_t/anuncios_transformados.parquet"
CAMINHO_DESTINO = "source_google/fato_anuncio/fato_anuncio.parquet"

# === Escolha dos campos a serem levados para o Gold ===
CAMPOS_DESEJADOS = [
    "data", "id_campanha", "id_grupo", 
    "id_anuncio","id_rede","id_dispositivo",
    "impressoes", "cliques", "ctr",
    "cpc_medio", "custo", "conversoes"
]

# === Criar cliente do Azure Blob ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
silver_container = blob_service_client.get_container_client(CONTAINER_SILVER)
gold_container = blob_service_client.get_container_client(CONTAINER_GOLD)

# === Ler arquivo único da Silver ===
print(f"🔄 Lendo: {CAMINHO_ORIGEM}")
blob_data = silver_container.download_blob(CAMINHO_ORIGEM).readall()
df = pl.read_parquet(BytesIO(blob_data))

# === Selecionar colunas desejadas ===
colunas_presentes = [col for col in CAMPOS_DESEJADOS if col in df.columns]
df = df.select(colunas_presentes)

# === Salvar no destino da Gold ===
buffer = BytesIO()
df.write_parquet(buffer)
buffer.seek(0)

gold_container.upload_blob(name=CAMINHO_DESTINO, data=buffer, overwrite=True)
print(f"✅ Arquivo final salvo em: {CAMINHO_DESTINO}")
