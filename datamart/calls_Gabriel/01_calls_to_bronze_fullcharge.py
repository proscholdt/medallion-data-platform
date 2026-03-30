import gspread
import polars as pl
from oauth2client.service_account import ServiceAccountCredentials
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from datetime import datetime
import os
from dotenv import load_dotenv
import time

# INICIAR CONTADOR DE TEMPO
inicio = time.time()

# === CARREGAR VARIÁVEIS DE AMBIENTE ===
load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
CAMINHO_PASTA_BLOB = "source_calls/calls"

SHEET_ID = "1zrPNDtIPIlIKINPpV2Vsp95VPHF_bQ7XguIDlxe2iS0"
ABA_DADOS = "Página1"

# === CONEXÃO COM AZURE BLOB STORAGE ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# === AUTENTICAÇÃO GOOGLE SHEETS ===
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    "etlaadssheets-130cef47c418.json", scope
)
gc = gspread.authorize(credentials)

# === LER PLANILHA COMO LISTA DE DICIONARIOS ===
planilha = gc.open_by_key(SHEET_ID)
aba = planilha.worksheet(ABA_DADOS)

cabecalho = aba.row_values(1)
valores = aba.get_all_values()[1:]

# Converte os dados para lista de dicionarios
dados = [dict(zip(cabecalho, linha)) for linha in valores]

# === TRANSFORMAR EM DATAFRAME POLARS ===
df = pl.DataFrame(dados)

# === GERAR NOME DO ARQUIVO CSV
nome_arquivo = "calls.csv"
caminho_blob = f"{CAMINHO_PASTA_BLOB}/{nome_arquivo}"

# === SALVAR CSV EM MEMÓRIA (BytesIO) ===
buffer = BytesIO()
df.write_csv(buffer)
buffer.seek(0)

# === FAZER UPLOAD PARA O AZURE BLOB STORAGE ===
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
container_client.upload_blob(name=caminho_blob, data=buffer, overwrite=True)

# === EXIBIR RESULTADO E TEMPO DE EXECUÇÃO ===
fim = time.time()
tempo_total = fim - inicio

print(f"✅ CSV salvo com sucesso no Azure Blob: {caminho_blob}")
print(f"⏱️ Tempo total de processamento: {tempo_total:.2f} segundos")
