import os
import hashlib
import io
import polars as pl
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_BRONZE = "bronze"
CONTAINER_SILVER = "silver"

# Conexão Azure Blob
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
bronze_client = blob_service_client.get_container_client(CONTAINER_BRONZE)
silver_client = blob_service_client.get_container_client(CONTAINER_SILVER)

# Caminhos
pasta_origem = 'source_voomp/voomp'
pasta_destino_vendas = 'source_voomp/f_vendas'
pasta_destino_projecao = 'source_voomp/f_projecao'

# Listar blobs
blobs = bronze_client.list_blobs(name_starts_with=pasta_origem + "/")

# interar sobre os bobs do tipo xlsx e transformar em um unico df

for blob in blobs:
    if blob.name.lower().endswith('.xlsx'):
        nomebase = os.path.splitext(os.path.basename(blob.name))[0]
        print(nomebase)

    downloader = bronze_client.download_blob(blob)
    blobData = io.BytesIO()
    downloader.readinto(blobData)
    blobData.seek(0)

df = pl.read_excel(blobData)
# print(df.columns)
df = df.with_columns(
    pl.col("CPF/CNPJ")
    .rank(method="dense")
    .alias("ID_Comprador")
)

df = df.rename({
    'ID Venda':'ID_Venda', 
    'ID Produto':'ID_Produto',
      'ID Oferta':'ID_Oferta',
        'ID Contrato':'ID_Contrato'
})


colunasID = []
colunasNOTID = []
colunasComprador = []
for idx, (col, dtype) in enumerate(zip(df.columns, df.dtypes)):
    if col.startswith('ID'):
        colunasID.append(col)
        # print(f"{idx}: {col}: {dtype} tem ID")

    elif 'comprador' in col.lower():
        colunasComprador.append(col)
        

    else:
        colunasNOTID.append(col)
        # print(f"{idx}: {col}: {dtype} não tem ID")

print('-------------------------------')
print('-------------------------------')
print(colunasID)
print('-------------------------------')
print('-------------------------------')
# print(colunasComprador)
# print('-------------------------------')
# print('-------------------------------')
# for i in colunasNOTID:
#     print(i)

df_dim_cliente = df['ID_Comprador','Nome do comprador', 'Email do comprador','CPF/CNPJ','Número de telefone']
print(len(df_dim_cliente))

df_dim_cliente = df_dim_cliente.unique()
print(f'deduplicados: {len(df_dim_cliente)}')
print(df_dim_cliente)