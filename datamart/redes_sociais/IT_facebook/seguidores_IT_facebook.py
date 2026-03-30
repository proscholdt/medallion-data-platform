import requests
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContentSettings
import os
from dotenv import load_dotenv

# === Carrega variáveis de ambiente (.env) ===
load_dotenv()

# === Credenciais do Facebook ===
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
PAGE_ID = os.getenv("FB_PAGE_ID_ITVALLEY")

# === Configuração Azure Blob ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
CAMINHO_DESTINO = "source_redesociais/IT_facebook"

# === Conecta ao Blob ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# === 1. Obter Page Access Token ===
url_token = f"https://graph.facebook.com/v19.0/{PAGE_ID}?fields=access_token&access_token={ACCESS_TOKEN}"
res_token = requests.get(url_token)
if res_token.status_code != 200:
    print("❌ Erro ao obter Page Access Token:")
    print(res_token.text)
    exit()

page_token = res_token.json().get("access_token")
if not page_token:
    print("❌ Token da página não retornado.")
    exit()

# === 2. Obter seguidores ===
url_followers = f"https://graph.facebook.com/v19.0/{PAGE_ID}?fields=followers_count&access_token={page_token}"
res_followers = requests.get(url_followers)
if res_followers.status_code != 200:
    print("❌ Erro ao obter número de seguidores:")
    print(res_followers.text)
    exit()

followers = res_followers.json().get("followers_count")
data_hoje = datetime.now().strftime("%Y-%m-%d")

# === 3. Montar resultado JSON ===
resultado = {
    "pagina": "ItValleySchool",
    "data": data_hoje,
    "seguidores": followers
}

# === 4. Enviar para o Azure Blob ===
nome_arquivo = f"seguidores_itvalleyFacebook_{data_hoje}.json"
blob_path = f"{CAMINHO_DESTINO}/{nome_arquivo}"

blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_path)
blob_client.upload_blob(
    json.dumps(resultado, indent=2, ensure_ascii=False),
    overwrite=True,
    content_settings=ContentSettings(content_type="application/json")
)

print(f"✅ Arquivo JSON enviado para Azure: {blob_path}")
print(f"📊 Seguidores: {followers} | Data: {data_hoje}")





