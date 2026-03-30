
# ==========================================================
# Script: Exportar Estágios e Pipelines do Pipedrive (JSON)
# Versão: Upload direto para Azure, sem salvar localmente
# ==========================================================

import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContentSettings
from io import BytesIO

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Pipedrive
PD_API_URL = os.getenv("PD_API_URL")
PD_API_KEY = os.getenv("PD_API_KEY")

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
DESTINATION_FOLDER = "source_pipedrive/all_stages_and_pipelines"

# Gerar timestamp para nome do arquivo
now = datetime.now()
timestamp_str = now.strftime("%Y%m%d_%H%M%S")
file_name = f"all_stages_and_pipelines_{timestamp_str}.json"
destination_path = f"{DESTINATION_FOLDER}/{file_name}"

# Função para buscar estágios e pipelines
def buscar_estagios_com_pipelines():
    url = f"{PD_API_URL}/stages?api_token={PD_API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if not data.get("success"):
        raise Exception("Erro ao buscar estágios.")

    estagios = data.get("data", [])
    resultado = []
    for estagio in estagios:
        resultado.append({
            "stage_id": estagio["id"],
            "stage_name": estagio["name"],
            "stage_order_nr": estagio["order_nr"],
            "pipeline_id": estagio["pipeline_id"],
            "pipeline_name": estagio.get("pipeline_name", "Desconhecido")
        })
    return resultado

# Executar a busca
print("🔍 Buscando estágios dos pipelines...")
tabela_estagios = buscar_estagios_com_pipelines()
print(f"✅ {len(tabela_estagios)} estágios encontrados.")

# Serializar para string e converter em bytes
json_str = json.dumps(tabela_estagios, ensure_ascii=False, indent=2)
json_bytes = BytesIO(json_str.encode("utf-8"))

# Conectar ao Blob Storage
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Upload direto do JSON para o Blob
print(f"☁️ Enviando para Azure Blob Storage em: {destination_path}")
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=destination_path)

blob_client.upload_blob(json_bytes, overwrite=True, content_settings=ContentSettings(content_type="application/json"))

print("✅ Upload concluído com sucesso!")


