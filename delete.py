import requests
import os
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Endpoint e parâmetros
url = "https://app-orion-dev.azurewebsites.net/api/azure-datalake/delete"
params = {
    "storage_account_name": "saactivecampaign",
    "file_system_name": "bronze",
    "directory_path": "source_facebook/facebook_camp"
}

# API Key do .env
API_KEY = os.getenv("X_API_KEY")  # substituí hífen por underscore

if not API_KEY:
    raise ValueError("A variável de ambiente 'X_API_KEY' não foi encontrada no .env")

# Cabeçalhos com a API Key
headers = {
    "accept": "application/json",
    "X-API-Key": API_KEY
}

# Requisição DELETE
response = requests.delete(url, headers=headers, params=params)

# Verificando a resposta
if response.status_code == 200:
    print("✅ Pasta deletada com sucesso!")
    print(response.json())
else:
    print(f"❌ Erro ao deletar a pasta: {response.status_code}")
    print(response.text)
