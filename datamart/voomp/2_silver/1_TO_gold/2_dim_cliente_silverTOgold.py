import os
import io
import polars as pl
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"

# Conexão Azure Blob
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
silver_client = blob_service_client.get_container_client(CONTAINER_SILVER)
gold_client = blob_service_client.get_container_client(CONTAINER_GOLD)

# Caminhos
pasta_f_vendas = 'source_voomp/f_vendas'
pasta_dim_cliente = 'source_voomp/dim_cliente'
nome_dim_cliente = 'dim_cliente.parquet'

# Listar arquivos na pasta f_vendas
blobs = silver_client.list_blobs(name_starts_with=pasta_f_vendas + '/')
blobs = [b for b in blobs if b.name.lower().endswith('.parquet')]

if len(blobs) != 1:
    raise Exception(f"❌ Esperado 1 arquivo, mas encontrei {len(blobs)} arquivos.")

blob = blobs[0]

print(f"🔍 Lendo: {blob.name}")

downloader = silver_client.download_blob(blob)
blob_data = io.BytesIO()
downloader.readinto(blob_data)
blob_data.seek(0)

pl_df = pl.read_parquet(blob_data)

# Selecionar colunas desejadas
pl_df = pl_df.select([
    'Nome do comprador',
    'Email do comprador',
    'Número de telefone',
    'Endereço físico',
    'ID_Cliente',
    'CPF/CNPJ'
])

# ✅ Remover duplicatas → pois é uma dimensão
pl_df = pl_df.unique()

# Salvar como Parquet
parquet_buffer = io.BytesIO()
pl_df.write_parquet(parquet_buffer)
parquet_buffer.seek(0)

# Upload para a Gold
destino_blob = f"{pasta_dim_cliente}/{nome_dim_cliente}"
gold_client.upload_blob(name=destino_blob, data=parquet_buffer, overwrite=True)

print(f"✅ Arquivo dim_cliente salvo na Gold: {CONTAINER_GOLD}/{destino_blob}")
