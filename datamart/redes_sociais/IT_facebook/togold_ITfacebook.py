import os
import json
import polars as pl
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# === Carregar variáveis de ambiente ===
load_dotenv()

# === Azure Blob Config ===
ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_BRONZE = "bronze"
CONTAINER_GOLD = "gold"
PASTA_ORIGEM = "source_redesociais/IT_facebook/"
PASTA_DESTINO = "source_redesociais/IT_facebook/dados_facebook.parquet"

# === Conexão Azure ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={ACCOUNT_NAME};"
    f"AccountKey={ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service = BlobServiceClient.from_connection_string(connection_string)
bronze_client = blob_service.get_container_client(CONTAINER_BRONZE)
gold_client = blob_service.get_container_client(CONTAINER_GOLD)

# === Listar arquivos JSON na pasta de origem ===
blobs = bronze_client.list_blobs(name_starts_with=PASTA_ORIGEM)
arquivos = [b.name for b in blobs if b.name.endswith(".json")]

if not arquivos:
    print("⚠️ Nenhum arquivo JSON encontrado na bronze.")
    exit()

# === Ler e concatenar todos os JSONs ===
dfs = []
for blob_name in arquivos:
    blob_data = bronze_client.download_blob(blob_name).readall()
    try:
        json_obj = json.loads(blob_data)
        if isinstance(json_obj, dict):
            json_obj = [json_obj]
        df = pl.DataFrame(json_obj)
        dfs.append(df)
        print(f"✅ Carregado: {blob_name}")
    except Exception as e:
        print(f"❌ Erro ao processar {blob_name}: {e}")

if not dfs:
    print("⚠️ Nenhum dado válido para salvar.")
    exit()

df_final = pl.concat(dfs, how="diagonal")

# === Escreve o Parquet em memória ===
buffer = BytesIO()
df_final.write_parquet(buffer)
buffer.seek(0)

# === Envia para a camada gold ===
blob_gold = gold_client.get_blob_client(PASTA_DESTINO)
blob_gold.upload_blob(buffer, overwrite=True)

print(f"📦 Arquivo final enviado para: {PASTA_DESTINO}")
print(f"📊 Total de registros: {df_final.shape[0]}")
