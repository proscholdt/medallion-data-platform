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
CAMINHO_ORIGEM = "source_google/anuncios"
CAMINHO_DESTINO = "source_google/transformados/anuncios_t/anuncios_transformados.parquet"

# === Colunas finais desejadas (ordem obrigatória) ===
CAMPOS_FINAIS = [
    "data", "id_campanha", "id_grupo", "id_anuncio",
    "nome_anuncio", "tipo_anuncio",
    "rede", "id_rede",
    "dispositivo", "id_dispositivo",
    "impressoes", "cliques", "ctr",
    "cpc_medio", "custo", "conversoes",
    "final_urls","youtube_video_id","youtube_url",
    "thumbnail_url"
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

# === Ler e concatenar todos os arquivos ===
dfs = []
for blob in silver_container.list_blobs(name_starts_with=CAMINHO_ORIGEM):
    if not blob.name.endswith(".parquet"):
        continue

    print(f"🔄 Lendo: {blob.name}")
    blob_data = silver_container.download_blob(blob.name).readall()
    df = pl.read_parquet(BytesIO(blob_data))

    # Adiciona colunas faltantes com None
    for col in CAMPOS_FINAIS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    # Ordena colunas conforme a ordem padrão
    df = df.select([col for col in CAMPOS_FINAIS if col in df.columns])

    dfs.append(df)

if not dfs:
    print("⚠️ Nenhum arquivo Parquet encontrado.")
    exit()

df_total = pl.concat(dfs, how="vertical")

# === Gerar IDs únicos para dispositivo e rede ===
dispositivos = df_total.select("dispositivo").unique().to_series().to_list()
redes = df_total.select("rede").unique().to_series().to_list()

mapa_dispositivo = {v: i + 1 for i, v in enumerate(dispositivos)}
mapa_rede = {v: i + 1 for i, v in enumerate(redes)}

df_total = df_total.with_columns([
    df_total["dispositivo"].replace(mapa_dispositivo).alias("id_dispositivo"),
    df_total["rede"].replace(mapa_rede).alias("id_rede")
])

# === Selecionar colunas finais (reafirma a ordem correta) ===
colunas_presentes = [col for col in CAMPOS_FINAIS if col in df_total.columns]
df_final = df_total.select(colunas_presentes)

# === Escrever resultado final no destino transformado ===
buffer = BytesIO()
df_final.write_parquet(buffer)
buffer.seek(0)

silver_container.upload_blob(name=CAMINHO_DESTINO, data=buffer, overwrite=True)
print(f"✅ Arquivo final salvo em: {CAMINHO_DESTINO}")









# import os
# import polars as pl
# from azure.storage.blob import BlobServiceClient
# from io import BytesIO
# from dotenv import load_dotenv

# # === Carregar variáveis de ambiente ===
# load_dotenv()

# # === Variáveis de conexão e caminhos ===
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

# CONTAINER_SILVER = "silver"
# CAMINHO_ORIGEM = "source_google/anuncios"
# CAMINHO_DESTINO = "source_google/transformados/anuncios_t/anuncios_transformados.parquet"

# # === Colunas finais desejadas ===
# CAMPOS_FINAIS = [
#     "data", "id_campanha", "id_grupo", "id_anuncio",
#     "nome_anuncio", "tipo_anuncio",
#     "rede", "id_rede",
#     "dispositivo", "id_dispositivo",
#     "impressoes", "cliques", "ctr",
#     "cpc_medio", "custo", "conversoes"
# ]

# # === Criar cliente do Azure Blob ===
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)
# silver_container = blob_service_client.get_container_client(CONTAINER_SILVER)

# # === Ler e concatenar todos os arquivos ===
# dfs = []
# for blob in silver_container.list_blobs(name_starts_with=CAMINHO_ORIGEM):
#     if not blob.name.endswith(".parquet"):
#         continue

#     print(f"🔄 Lendo: {blob.name}")
#     blob_data = silver_container.download_blob(blob.name).readall()
#     df = pl.read_parquet(BytesIO(blob_data))
#     dfs.append(df)

# if not dfs:
#     print("⚠️ Nenhum arquivo Parquet encontrado.")
#     exit()

# df_total = pl.concat(dfs, how="vertical")

# # === Gerar IDs únicos para dispositivo e rede ===
# dispositivos = df_total.select("dispositivo").unique().to_series().to_list()
# redes = df_total.select("rede").unique().to_series().to_list()

# mapa_dispositivo = {v: i + 1 for i, v in enumerate(dispositivos)}
# mapa_rede = {v: i + 1 for i, v in enumerate(redes)}

# df_total = df_total.with_columns([
#     df_total["dispositivo"].replace(mapa_dispositivo).alias("id_dispositivo"),
#     df_total["rede"].replace(mapa_rede).alias("id_rede")
# ])

# # === Selecionar colunas finais ===
# colunas_presentes = [col for col in CAMPOS_FINAIS if col in df_total.columns]
# df_final = df_total.select(colunas_presentes)

# # === Escrever resultado final no destino transformado ===
# buffer = BytesIO()
# df_final.write_parquet(buffer)
# buffer.seek(0)

# silver_container.upload_blob(name=CAMINHO_DESTINO, data=buffer, overwrite=True)
# print(f"✅ Arquivo final salvo em: {CAMINHO_DESTINO}")
