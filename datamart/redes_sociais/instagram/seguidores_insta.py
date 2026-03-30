import requests
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContentSettings
import os
from dotenv import load_dotenv

# === Carrega variáveis de ambiente (.env) ===
load_dotenv()

# === Credenciais ===
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
PAGE_ID = os.getenv("FB_PAGE_ID_CARLOSVIANA")  # Carlos Viana

# === Azure Blob Config ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
CAMINHO_DESTINO = "source_redesociais/instagram"

connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# === 1. Obter Instagram vinculado à Página ===
url_ig_account = f"https://graph.facebook.com/v19.0/{PAGE_ID}?fields=instagram_business_account&access_token={ACCESS_TOKEN}"
res_ig = requests.get(url_ig_account)

if res_ig.status_code != 200:
    print("❌ Erro ao obter conta Instagram vinculada:")
    print(res_ig.text)
    exit()

ig_account = res_ig.json().get("instagram_business_account", {}).get("id")
if not ig_account:
    print("⚠️ Nenhuma conta Instagram vinculada à página.")
    exit()

# === 2. Obter seguidores ===
url_followers = f"https://graph.facebook.com/v19.0/{ig_account}?fields=followers_count,username&access_token={ACCESS_TOKEN}"
res_followers = requests.get(url_followers)

if res_followers.status_code != 200:
    print("❌ Erro ao obter seguidores do Instagram:")
    print(res_followers.text)
    exit()

data = res_followers.json()
username = data.get("username")
followers = data.get("followers_count")
data_hoje = datetime.now().strftime("%Y-%m-%d")

resultado = {
    "conta": username,
    "data": data_hoje,
    "seguidores": followers
}

# === 3. Enviar JSON para Azure Blob ===
nome_arquivo = f"seguidores_instagram_{username}_{data_hoje}.json"
blob_path = f"{CAMINHO_DESTINO}/{nome_arquivo}"

blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_path)
blob_client.upload_blob(
    json.dumps(resultado, indent=2, ensure_ascii=False),
    overwrite=True,
    content_settings=ContentSettings(content_type="application/json")
)

print(f"📸 Conta Instagram: @{username}")
print(f"✅ Seguidores: {followers} | Data: {data_hoje}")
print(f"☁️ Arquivo enviado para Azure: {blob_path}")
