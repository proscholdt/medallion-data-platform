# ==========================================================
# Integração completa: Processa JSONs da deal move bronze, salva Parquet na Silver e move/exclui blobs processados na bronze.
# Adiciona coluna fk_allstages e deal_ativo (1 = ativo no Pipe; 0 = não existe).
# ==========================================================

import os
import json
import requests
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import polars as pl
from io import BytesIO
from datetime import datetime

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
SOURCE_CONTAINER = "bronze"
DEST_CONTAINER = "silver"
SOURCE_FOLDER = "source_pipedrive/deal_move"
DEST_FOLDER = "source_pipedrive/deal_move"
CARREGADOS_FOLDER = "source_pipedrive/carregados_deal_move"

# Configurações Pipedrive
PD_API_URL = os.getenv("PD_API_URL")
PD_API_KEY = os.getenv("PD_API_KEY")

# Gerar nome dinâmico para o arquivo Parquet
now = datetime.now()
data_atual = now.strftime("%Y%m%d")
hora_atual = now.strftime("%H%M%S")
OUTPUT_FILE_NAME = f"deal_move_{data_atual}_{hora_atual}.parquet"

# Conectar ao Azure
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
source_container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
dest_container_client = blob_service_client.get_container_client(DEST_CONTAINER)

# Função para buscar mapeamento de campos personalizados da pessoa
def buscar_mapeamento_personfields():
    url = f"{PD_API_URL}/personFields?api_token={PD_API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if not data.get("success"):
        raise Exception("Erro ao buscar campos personalizados da pessoa.")
    return {campo["key"]: campo["name"] for campo in data["data"]}

# Função para buscar dados UTM da pessoa
def buscar_utm_da_pessoa(person_id, mapeamento_campos):
    utm_data = {
        "utm_campaign": "",
        "utm_source": "",
        "utm_medium": "",
        "utm_content": ""
    }
    if not person_id:
        return utm_data
    try:
        url = f"{PD_API_URL}/persons/{person_id}?api_token={PD_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        pessoa_dados = response.json().get("data", {})

        utm_aliases = {
            "utm campaign": "utm_campaign",
            "utm_campaign": "utm_campaign",
            "utm source": "utm_source",
            "utm_source": "utm_source",
            "utm medium": "utm_medium",
            "utm_medium": "utm_medium",
            "utm content": "utm_content",
            "utm_content": "utm_content"
        }

        for key, value in pessoa_dados.items():
            nome_legivel = mapeamento_campos.get(key, key)
            nome_normalizado = nome_legivel.strip().lower().replace("_", " ")
            if nome_normalizado in utm_aliases:
                campo_final = utm_aliases[nome_normalizado]
                utm_data[campo_final] = value or ""

    except Exception as e:
        print(f"⚠️ Erro ao buscar UTMs da pessoa {person_id}: {e}")
    return utm_data

# Função para buscar todos os deals ativos (pegando todos os deals via paginação)
def buscar_todos_deals_ativos():
    print("🔄 Buscando todos os deals ativos no Pipedrive...")
    todos_deals = set()
    start = 0
    while True:
        url = f"{PD_API_URL}/deals?start={start}&limit=100&api_token={PD_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        deals = data.get("data", [])
        if not deals:
            break

        for deal in deals:
            if deal and deal.get("id"):
                todos_deals.add(str(deal["id"]))  # Cast para string para bater com a coluna castada depois

        # Paginação
        pagination = data.get("additional_data", {}).get("pagination", {})
        if not pagination.get("more_items_in_collection"):
            break
        start = pagination.get("next_start", 0)
    print(f"✅ {len(todos_deals)} deals ativos encontrados.\n")
    return todos_deals

# Buscar mapeamento dos campos personalizados das pessoas
print("🔍 Buscando mapeamento de campos personalizados da pessoa...")
mapeamento_campos = buscar_mapeamento_personfields()
print("✅ Mapeamento capturado.")

# Buscar todos os deals ativos
deals_ativos_ids = buscar_todos_deals_ativos()

# Listar arquivos JSON no diretório de origem
blobs = source_container_client.list_blobs(name_starts_with=SOURCE_FOLDER)
json_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

print(f"🔎 {len(json_files)} arquivos JSON encontrados.")

# Lista das colunas finais obrigatórias + UTM + nova coluna fk_allstages + deal_ativo
colunas_finais = [
    "event_type",
    "deal_id",
    "pipeline_id",
    "old_stage_id",
    "new_stage_id",
    "title",
    "value",
    "currency",
    "person_id",
    "owner_id",
    "status",
    "pipeline_name",
    "movement_type",
    "comment",
    "timestamp",  # agora como string
    "email",
    "utm_campaign",
    "utm_source",
    "utm_medium",
    "utm_content",
    "fk_allstages",
    "deal_ativo"
]

# Lista para acumular DataFrames e rastrear blobs processados com sucesso
dfs = []
blobs_processados_sucesso = []

# Loop para processar cada JSON e padronizar os dados
for json_file in json_files:
    try:
        blob_client = source_container_client.get_blob_client(json_file)
        blob_data = blob_client.download_blob().readall()
        df = pl.read_json(BytesIO(blob_data))

        # Renomear old_value e new_value
        rename_dict = {}
        if "old_value" in df.columns:
            rename_dict["old_value"] = "old_stage_id"
        if "new_value" in df.columns:
            rename_dict["new_value"] = "new_stage_id"
        if rename_dict:
            df = df.rename(rename_dict)
            print(f"🔄 Renomeadas colunas em {json_file}: {rename_dict}")

        # Buscar UTM com base no person_id
        person_id = df.get_column("person_id")[0] if "person_id" in df.columns else None
        utm_dados = buscar_utm_da_pessoa(person_id, mapeamento_campos)

        # Cast de todas as colunas existentes para string (Utf8)
        for col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

        # Se timestamp não existir, cria como string vazia
        if "timestamp" not in df.columns:
            df = df.with_columns(pl.lit("").alias("timestamp").cast(pl.Utf8))

        # Adicionar os campos UTM enriquecidos como string
        df = df.with_columns([
            pl.lit(utm_dados.get("utm_campaign", "")).alias("utm_campaign").cast(pl.Utf8),
            pl.lit(utm_dados.get("utm_source", "")).alias("utm_source").cast(pl.Utf8),
            pl.lit(utm_dados.get("utm_medium", "")).alias("utm_medium").cast(pl.Utf8),
            pl.lit(utm_dados.get("utm_content", "")).alias("utm_content").cast(pl.Utf8)
        ])

        # Criar a coluna fk_allstages: concatena new_stage_id + "|" + pipeline_id
        df = df.with_columns(
            (pl.col("new_stage_id").cast(pl.Utf8) + "|" + pl.col("pipeline_id").cast(pl.Utf8))
            .alias("fk_allstages")
        )

        # Criar a coluna deal_ativo (1 = ativo, 0 = não existe)
        df = df.with_columns(
            pl.when(pl.col("deal_id").is_in(list(deals_ativos_ids)))
            .then(pl.lit("1"))
            .otherwise(pl.lit("0"))
            .alias("deal_ativo")
        )

        # Preencher outras colunas faltantes com string vazia
        colunas_faltantes = [col for col in colunas_finais if col not in df.columns]
        for col_faltante in colunas_faltantes:
            df = df.with_columns(pl.lit("").alias(col_faltante).cast(pl.Utf8))

        # Reordenar para manter a ordem correta
        df = df.select(colunas_finais)

        dfs.append(df)
        blobs_processados_sucesso.append(json_file)

    except Exception as e:
        print(f"❌ Erro ao processar {json_file}: {e}")

# Se processou pelo menos 1 arquivo, salva o Parquet e faz o pós-processamento
if dfs:
    df_final = pl.concat(dfs, how="vertical")

    output_buffer = BytesIO()
    df_final.write_parquet(output_buffer)
    output_buffer.seek(0)

    dest_blob_client = dest_container_client.get_blob_client(f"{DEST_FOLDER}/{OUTPUT_FILE_NAME}")
    dest_blob_client.upload_blob(output_buffer, overwrite=True)
    print(f"✅ Arquivo Parquet salvo com sucesso em: {DEST_FOLDER}/{OUTPUT_FILE_NAME}")

    # ---- Mover e deletar os blobs processados ----
    print(f"🚚 Iniciando movimentação dos arquivos processados ({len(blobs_processados_sucesso)})...\n")
    for i, blob_name in enumerate(blobs_processados_sucesso, 1):
        try:
            source_blob_client = source_container_client.get_blob_client(blob_name)
            data = source_blob_client.download_blob().readall()

            file_name_only = os.path.basename(blob_name)
            dest_blob_name = f"{CARREGADOS_FOLDER}/{file_name_only}"
            dest_blob_client = source_container_client.get_blob_client(dest_blob_name)
            dest_blob_client.upload_blob(data, overwrite=True)

            print(f"✅ [{i}] Movido para: {dest_blob_name}")

            # Deletar o blob original após mover
            source_blob_client.delete_blob()
            print(f"🗑️ [{i}] Deletado da pasta de origem: {blob_name}\n")

        except Exception as e:
            print(f"❌ Erro ao mover/deletar {blob_name}: {e}")

    print("\n🎉 Processamento e movimentação finalizada com sucesso.")

else:
    print("⚠️ Nenhum arquivo foi processado com sucesso. Nada para salvar ou mover.")
