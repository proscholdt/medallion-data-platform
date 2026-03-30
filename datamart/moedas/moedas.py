import os
import requests
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from dotenv import load_dotenv

# === CARREGAR VARIÁVEIS DE AMBIENTE ===
load_dotenv()

# === CONFIGURAÇÕES AZURE ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "gold"
FOLDER_NAME = "Moedas"

# === CHAVE DE ACESSO DA API ===
API_ACCESS_KEY = os.getenv("API_ACCESS_KEY")

# === VERIFICAÇÃO DE VARIÁVEIS ===
if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, API_ACCESS_KEY]):
    raise ValueError("❌ Variáveis de ambiente ausentes. Verifique o arquivo .env")

# === HEADERS PARA A API ===
HEADERS = {
    "apikey": API_ACCESS_KEY
}

# === BASE URL DA API ===
BASE_URL = "https://api.apilayer.com/exchangerates_data"

# === CONEXÃO COM AZURE ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# === LISTAGEM DE ARQUIVOS PARQUET EXISTENTES ===
print(f"🔍 Verificando arquivos existentes em {FOLDER_NAME}/")
blobs = container_client.list_blobs(name_starts_with=FOLDER_NAME + "/")
blobs_parquet = [blob.name for blob in blobs if blob.name.endswith(".parquet")]

# === ENCONTRAR MAIOR DATA ENTRE OS BLOBS ===
import re
data_pattern = re.compile(r"cotacoes_brl_historico_(\d{4}-\d{2}-\d{2})\.parquet$")
datas_blob = []
for blob_name in blobs_parquet:
    match = data_pattern.search(blob_name)
    if match:
        datas_blob.append(match.group(1))

if not datas_blob:
    raise ValueError("❌ Nenhum arquivo de cotação encontrado no Data Lake!")

maior_data_blob = max(datas_blob)
print(f"📅 Maior data encontrada nos blobs: {maior_data_blob}")

# === FUNÇÃO PARA BUSCAR COTAÇÕES ===
def buscar_cotacoes_historicas(base_currency="BRL", symbols=["USD", "CAD", "EUR"], start_date="2024-08-01"):
    # Recebe datas como string YYYY-MM-DD
    from datetime import date, timedelta
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = date.today()
    symbols_str = ",".join(symbols)
    resultados = []
    for single_date in pd.date_range(start_dt, end_dt):
        data_str = single_date.strftime("%Y-%m-%d")
        url = (
            f"{BASE_URL}/latest?base={base_currency}&symbols={symbols_str}&date={data_str}"
        )
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"❌ Erro HTTP {response.status_code} para {data_str}")
            print(f"🔎 Conteúdo da resposta: {response.text}")
            continue
        try:
            data = response.json()
        except Exception as e:
            print(f"❌ Erro ao decodificar JSON: {e} para {data_str}")
            continue
        if not data.get("success", False):
            print(f"❌ Erro na resposta da API para {data_str}: {data}")
            continue
        taxas = data.get('rates', {})
        for moeda, valor in taxas.items():
            resultados.append({
                'data_extracao': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'data_referencia': data_str,
                'moeda_origem': base_currency,
                'moeda_destino': moeda,
                'quantidade': 1,
                'resultado': valor
            })
    return resultados

# === EXECUÇÃO PRINCIPAL ===
# === EXECUÇÃO PRINCIPAL ===
from datetime import timedelta
maior_data_dt = datetime.strptime(maior_data_blob, "%Y-%m-%d").date()
start_date_plus1 = (maior_data_dt + timedelta(days=1)).strftime("%Y-%m-%d")
print(f"🚀 Extraindo cotações de {start_date_plus1} até hoje...")
dados = buscar_cotacoes_historicas(start_date=start_date_plus1)
df = pd.DataFrame(dados)

# === GERAR ARQUIVO PARQUET COM COMPACTAÇÃO ===
buffer = BytesIO()
df.to_parquet(buffer, index=False, compression="snappy")
buffer.seek(0)

# === NOME E UPLOAD DO ARQUIVO ===
data_hoje = datetime.now().strftime("%Y-%m-%d")
nome_arquivo = f"{FOLDER_NAME}/cotacoes_brl_historico_{data_hoje}.parquet"
blob_client = container_client.get_blob_client(nome_arquivo)
blob_client.upload_blob(buffer, overwrite=True)

print(f"✅ Arquivo {nome_arquivo} enviado com sucesso para o Data Lake!")






