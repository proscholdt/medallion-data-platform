import gspread
import polars as pl
from oauth2client.service_account import ServiceAccountCredentials
from azure.storage.blob import BlobServiceClient
from io import StringIO
from datetime import datetime
import os
from dotenv import load_dotenv

# === CARREGAR VARIÁVEIS DE AMBIENTE ===
load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
CAMINHO_PASTA_BLOB = "source_google/campanhas"

SHEET_ID = "1doaKLuEuJqu4PFte0lp9nr98MvVkZ1THy5-agzYMF1o"
ABA_DADOS = "resumo_campanhas"
ABA_CONTROLE = "controle"

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

# === LEITURA DE DADOS ===
planilha = gc.open_by_key(SHEET_ID)
aba = planilha.worksheet(ABA_DADOS)
cabecalho = aba.row_values(1)
valores = aba.get_all_values()[1:]
dados = [dict(zip(cabecalho, linha)) for linha in valores]

# === COLUNAS NUMÉRICAS ===
colunas_float = ["ctr", "cpc_medio", "custo", "conversoes"]

# === NORMALIZAÇÃO ===
todas_as_colunas = set(cabecalho)
dados_normalizados = []
for linha in dados:
    nova_linha = {}
    for col in todas_as_colunas:
        valor = str(linha.get(col, "")).strip()
        if valor.lower() in ("", "none", "nan"):
            nova_linha[col] = "0"
        elif col in colunas_float:
            nova_linha[col] = valor.replace(",", ".")
        else:
            nova_linha[col] = valor
    dados_normalizados.append(nova_linha)

# === CRIA DATAFRAME POLARS ===
df = pl.DataFrame(dados_normalizados)

# === CAST FLOAT ===
for col in colunas_float:
    if col in df.columns:
        df = df.with_columns(
            pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0).alias(col)
        )

# === CONVERTE DATA ===
df = df.with_columns(
    pl.col("data").cast(str).str.strip_chars().str.strptime(pl.Date, "%Y-%m-%d", strict=False)
)
data_max = df.select(pl.col("data").max()).item().strftime("%Y-%m-%d")

# === ATUALIZA CONTROLE ===
controle = planilha.worksheet(ABA_CONTROLE)
controle_dados = controle.get_all_values()
atualizado = False
for i, linha in enumerate(controle_dados):
    if linha[0].strip().lower() == ABA_DADOS:
        controle.update_cell(i + 1, 2, data_max)
        atualizado = True
        break
if not atualizado:
    controle.append_row([ABA_DADOS, data_max])
    print(f"⚠️ Controle não tinha linha para '{ABA_DADOS}', foi criada.")

# === SALVA COMO CSV E ENVIA PARA BLOB ===
data_hoje_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
nome_arquivo_csv = f"{data_hoje_str}_{ABA_DADOS}.csv"
CAMINHO_BLOB = f"{CAMINHO_PASTA_BLOB}/{nome_arquivo_csv}"

buffer = StringIO()
df.write_csv(buffer)
buffer.seek(0)

blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=CAMINHO_BLOB)
blob_client.upload_blob(buffer.getvalue(), overwrite=True)

print(f"✅ CSV gerado com data {data_max} e enviado para: {CONTAINER_NAME}/{CAMINHO_BLOB}")
