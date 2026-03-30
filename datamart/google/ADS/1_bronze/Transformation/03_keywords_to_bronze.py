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

# === CREDENCIAIS AZURE ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
CAMINHO_PASTA_BLOB = "source_google/keywords"

# === CONEXÃO COM AZURE BLOB STORAGE ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# === CONFIGURAÇÕES GOOGLE SHEETS ===
SHEET_ID = "1doaKLuEuJqu4PFte0lp9nr98MvVkZ1THy5-agzYMF1o"
ABA_DADOS = "keywords"
ABA_CONTROLE = "controle"

# === AUTENTICAÇÃO GOOGLE SHEETS ===
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    "etlaadssheets-130cef47c418.json", scope
)
gc = gspread.authorize(credentials)

# === LEITURA DE DADOS COMO TEXTO ===
planilha = gc.open_by_key(SHEET_ID)
aba = planilha.worksheet(ABA_DADOS)
cabecalho = aba.row_values(1)
valores = aba.get_all_values()[1:]
dados = [dict(zip(cabecalho, linha)) for linha in valores]

# === COLUNAS FLOAT COM VÍRGULA ===
colunas_float = ["ctr", "cpc_medio", "custo", "conversoes"]

# === NORMALIZAÇÃO DOS DADOS ===
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

# === CRIA DATAFRAME COM TIPOS CORRETOS ===
df = pl.DataFrame(dados_normalizados)

# === CAST DAS COLUNAS NUMÉRICAS ===
for col in colunas_float:
    if col in df.columns:
        df = df.with_columns(
            pl.col(col)
            .cast(pl.Float64, strict=False)
            .fill_null(0.0)
            .alias(col)
        )

# === CONVERTE COLUNA "data" PARA DATA E EXTRAI MAIOR DATA ===
try:
    df = df.with_columns(
        pl.col("data").cast(str).str.strip_chars().str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("data")
    )
    data_max = df.select(pl.col("data").max()).item().strftime("%Y-%m-%d")
except Exception as e:
    print(f"❌ Erro ao extrair data mais recente: {e}")
    data_max = datetime.now().strftime("%Y-%m-%d")

# === ATUALIZA A PLANILHA DE CONTROLE ===
controle = planilha.worksheet(ABA_CONTROLE)
controle_dados = controle.get_all_values()
atualizado = False
for i, linha in enumerate(controle_dados):
    if linha[0].strip().lower() == ABA_DADOS:
        controle.update_cell(i + 1, 2, data_max)
        atualizado = True
        break
if not atualizado:
    print(f"⚠️ Linha de controle para '{ABA_DADOS}' não encontrada.")

# === GERA CSV E ENVIA PARA O BLOB STORAGE ===
data_hoje_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
NOME_ARQUIVO_CSV = f"{data_hoje_str}_keywords.csv"
CAMINHO_BLOB = f"{CAMINHO_PASTA_BLOB}/{NOME_ARQUIVO_CSV}"

buffer = StringIO()
df.write_csv(buffer)
buffer.seek(0)

blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=CAMINHO_BLOB)
blob_client.upload_blob(buffer.getvalue(), overwrite=True)

print(f"✅ CSV gerado com data {data_max} e enviado para: {CONTAINER_NAME}/{CAMINHO_BLOB}")
