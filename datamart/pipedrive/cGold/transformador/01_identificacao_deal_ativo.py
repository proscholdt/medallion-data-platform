import os
import requests
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import polars as pl
from io import BytesIO

# Carregar variáveis de ambiente
load_dotenv()

# Configuração Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "gold"
PASTA_PARQUET = "source_pipedrive/ft_move/"

# Configuração Pipedrive
PD_API_URL = os.getenv("PD_API_URL")
PD_API_KEY = os.getenv("PD_API_KEY")

# Conectar ao Azure Blob Storage
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Buscar todos os deals não deletados
def buscar_deals_nao_deletados():
    print("🔄 Buscando todos os deals não deletados no Pipedrive...")
    active_ids = set()
    start = 0
    while True:
        url = f"{PD_API_URL}/deals?status=all_not_deleted&start={start}&limit=100&api_token={PD_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        deals = data.get("data", [])
        if not deals:
            break
        for deal in deals:
            if deal.get("id"):
                active_ids.add(str(deal["id"]))  # sempre como string
        if not data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
            break
        start = data["additional_data"]["pagination"].get("next_start", 0)
    print(f"✅ {len(active_ids)} deals não deletados encontrados.\n")
    return active_ids

# Buscar IDs não deletados
deals_ativos_ids = buscar_deals_nao_deletados()

# Listar Parquets
blobs = container_client.list_blobs(name_starts_with=PASTA_PARQUET)
parquet_files = [blob.name for blob in blobs if blob.name.endswith(".parquet")]

print(f"🔎 {len(parquet_files)} arquivos encontrados.\n")

# Processar cada Parquet
for i, blob_name in enumerate(parquet_files, 1):
    try:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        parquet_data = blob_client.download_blob().readall()
        df = pl.read_parquet(BytesIO(parquet_data))

        # Garantir que deal_id é string para comparar corretamente
        df = df.with_columns(
            pl.col("deal_id").cast(pl.Utf8).alias("deal_id_str")
        )

        # Atualizar flag_deal_ativo: 1 se NÃO deletado, 0 se deletado
        df = df.with_columns(
            pl.when(pl.col("deal_id_str").is_in(deals_ativos_ids))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("flag_deal_ativo")
        )

        # Remover coluna auxiliar (opcional)
        df = df.drop("deal_id_str")

        # Salvar novamente
        output_buffer = BytesIO()
        df.write_parquet(output_buffer)
        output_buffer.seek(0)
        blob_client.upload_blob(output_buffer, overwrite=True)

        print(f"✅ [{i}] {blob_name} atualizado com sucesso.\n")

    except Exception as e:
        print(f"❌ [{i}] Erro ao processar {blob_name}: {e}")

print("🎉 Finalizado! Todos os arquivos foram processados.")
