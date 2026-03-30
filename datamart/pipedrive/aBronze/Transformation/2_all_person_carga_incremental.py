





import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from io import BytesIO

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
DESTINATION_FOLDER = "source_pipedrive/person_all"

# Configurações Pipedrive
PD_API_URL = os.getenv("PD_API_URL")
PD_API_KEY = os.getenv("PD_API_KEY")

# Caminho para o CSV local de controle
CSV_CONTROLE = "data_person_pipedrive.csv"

# Conexão com o Azure
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Timestamp
now = datetime.now()
data_atual = now.strftime("%Y%m%d")
hora_atual = now.strftime("%H%M%S")

# === 1. Carregar CSV de controle ===
if os.path.exists(CSV_CONTROLE):
    df_controle = pd.read_csv(CSV_CONTROLE)
    ids_ja_carregados = set(df_controle["person_id"])
else:
    ids_ja_carregados = set()
    df_controle = pd.DataFrame(columns=["person_id"])

print(f"🔍 {len(ids_ja_carregados)} pessoas já carregadas registradas no CSV.")

# === 2. Buscar todos os persons via API ===
def buscar_todas_pessoas():
    pessoas = []
    start = 0
    while True:
        url = f"{PD_API_URL}/persons?start={start}&limit=100&api_token={PD_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if not data.get("success"):
            raise Exception("Erro ao buscar pessoas.")

        dados_pagina = data.get("data", [])
        if not dados_pagina:
            break

        pessoas.extend(dados_pagina)
        print(f"🔄 {len(pessoas)} pessoas carregadas até agora...")

        if not data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
            break

        start = data["additional_data"]["pagination"]["next_start"]
    return pessoas

print("🔍 Buscando todas as pessoas no Pipedrive...")
todas_pessoas = buscar_todas_pessoas()
print(f"✅ {len(todas_pessoas)} pessoas encontradas.")

# === 3. Filtrar novas pessoas ===
novas_pessoas = [p for p in todas_pessoas if p.get("id") not in ids_ja_carregados]
print(f"🚀 {len(novas_pessoas)} pessoas novas para exportar.")

# === 4. Exportar novos JSONs e atualizar CSV ===
novos_ids = []
for pessoa in novas_pessoas:
    person_id = pessoa.get("id")
    if not person_id:
        continue

    file_name = f"{DESTINATION_FOLDER}/person_{person_id}_{data_atual}_{hora_atual}.json"
    content = json.dumps(pessoa, indent=2, ensure_ascii=False).encode("utf-8")
    stream = BytesIO(content)

    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=file_name)
    blob_client.upload_blob(stream, overwrite=True)

    print(f"📤 Pessoa {person_id} exportada com sucesso. Arquivo: {file_name}")
    novos_ids.append({"person_id": person_id})

# === 5. Atualizar o CSV local ===
if novos_ids:
    df_novos = pd.DataFrame(novos_ids)
    df_controle = pd.concat([df_controle, df_novos], ignore_index=True).drop_duplicates()
    df_controle.to_csv(CSV_CONTROLE, index=False)
    print(f"✅ CSV '{CSV_CONTROLE}' atualizado com {len(df_controle)} registros.")

print("\n🎯 Processo concluído.")
