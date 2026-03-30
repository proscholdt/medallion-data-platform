import os
import polars as pl
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from dotenv import load_dotenv

# === Carregar variáveis de ambiente ===
load_dotenv()

# === Conexão com Azure Blob ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"
CAMINHO_SILVER = "source_google/campanhas"
CAMINHO_GOLD = "source_google/fato_campanha"
NOME_ARQUIVO_GOLD = "fato_campanha.parquet"

# Colunas desejadas (ordem obrigatória)
CAMPOS_DESEJADOS = [
    "data", "id_campanha", "status_campanha",
    "impressoes", "cliques", "ctr",
    "cpc_medio", "custo", "conversoes"
]

# === Criar cliente Azure ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

silver_container = blob_service_client.get_container_client(CONTAINER_SILVER)
gold_container = blob_service_client.get_container_client(CONTAINER_GOLD)

# === Ler todos os arquivos Parquet da Silver ===
dfs = []
for blob in silver_container.list_blobs(name_starts_with=CAMINHO_SILVER):
    if not blob.name.endswith(".parquet"):
        continue

    print(f"🔄 Lendo: {blob.name}")
    blob_data = silver_container.download_blob(blob.name).readall()
    df = pl.read_parquet(BytesIO(blob_data))

    # Verifica se todas as colunas desejadas estão presentes
    if all(col in df.columns for col in CAMPOS_DESEJADOS):
        # Reordena conforme CAMPOS_DESEJADOS
        df = df.select(CAMPOS_DESEJADOS)
        dfs.append(df)
    else:
        print(f"⚠️ Ignorado: {blob.name} — colunas incompletas")

# === Consolidar e salvar no destino Gold com nome fixo ===
if dfs:
    df_final = pl.concat(dfs, how="vertical")

    buffer = BytesIO()
    df_final.write_parquet(buffer)
    buffer.seek(0)

    caminho_destino = f"{CAMINHO_GOLD}/{NOME_ARQUIVO_GOLD}"
    gold_container.upload_blob(name=caminho_destino, data=buffer, overwrite=True)
    print(f"✅ Arquivo salvo em: {caminho_destino}")
else:
    print("⚠️ Nenhum arquivo válido encontrado na Silver.")




