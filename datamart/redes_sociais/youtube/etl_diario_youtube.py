import os
import json
from datetime import datetime
from io import BytesIO
from azure.storage.blob import BlobServiceClient, ContentSettings
from googleapiclient.discovery import build
from dotenv import load_dotenv

# === Carrega variáveis de ambiente ===
load_dotenv()

# === YouTube Config ===
API_KEY_youtube = os.getenv("API_KEY_youtube")
CHANNEL_ID = "UC_g3YMZKkdIyMljUaRgg4qQ"

# === Azure Blob Config ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
CAMINHO_DESTINO = "source_redesociais/youtube"

# === Data e nome do arquivo ===
data_hoje = datetime.now().strftime("%Y-%m-%d")
nome_arquivo = f"{data_hoje}_dadosYoutube.json"
blob_path = f"{CAMINHO_DESTINO}/{nome_arquivo}"

# === Consulta à API do YouTube ===
youtube = build("youtube", "v3", developerKey=API_KEY_youtube)
res = youtube.channels().list(part="snippet,statistics", id=CHANNEL_ID).execute()
item = res["items"][0]

# === Dados extraídos ===
dados = {
    "data": data_hoje,
    "canal": item["snippet"]["title"],
    "inscritos": int(item["statistics"]["subscriberCount"]),
    "visualizacoes": int(item["statistics"]["viewCount"])
}

# === Conectar ao Azure Blob ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_path)

# === Enviar JSON para Azure ===
json_bytes = json.dumps(dados, indent=2, ensure_ascii=False).encode("utf-8")
blob_client.upload_blob(
    json_bytes,
    overwrite=True,
    content_settings=ContentSettings(content_type="application/json")
)

print(f"✅ Arquivo JSON enviado para Azure: {blob_path}")
print(f"📺 Canal: {dados['canal']} | Inscritos: {dados['inscritos']} | Visualizações: {dados['visualizacoes']}")
