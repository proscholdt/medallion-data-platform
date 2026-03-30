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
CAMINHO_PASTA_BLOB = "source_google/grupos_de_anuncio"

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
ABA_DADOS = "resumo_grupos"
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

# === LEITURA COMO TEXTO ===
planilha = gc.open_by_key(SHEET_ID)
aba = planilha.worksheet(ABA_DADOS)
cabecalho = aba.row_values(1)
valores = aba.get_all_values()[1:]  # ignora o cabeçalho
dados = [dict(zip(cabecalho, linha)) for linha in valores]

# === COLUNA FLOAT A SER TRATADA ===
colunas_float = ["ctr", "cpc_medio", "custo", "conversoes"]

# === NORMALIZAÇÃO: tudo como string, vírgula para ponto na coluna custo ===
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

# === CONVERTE PARA DATAFRAME POLARS ===
df = pl.DataFrame(dados_normalizados)

# === CAST PARA FLOAT SOMENTE EM 'custo' ===
for col in colunas_float:
    if col in df.columns:
        df = df.with_columns(
            pl.col(col)
            .cast(pl.Float64, strict=False)
            .fill_null(0.0)
            .alias(col)
        )

# === OBTÉM A DATA MAIS RECENTE DA COLUNA "data" ===
try:
    df = df.with_columns(
        pl.col("data").cast(str).str.strip_chars().str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("data")
    )
    data_max = df.select(pl.col("data").max()).item().strftime("%Y-%m-%d")
except Exception as e:
    print(f"❌ Erro ao extrair data mais recente: {e}")
    data_max = datetime.now().strftime("%Y-%m-%d")

# === ATUALIZA PLANILHA DE CONTROLE ===
controle = planilha.worksheet(ABA_CONTROLE)
controle_dados = controle.get_all_values()
for i, linha in enumerate(controle_dados):
    if linha[0].strip().lower() == ABA_DADOS:
        controle.update_cell(i + 1, 2, data_max)
        break

# === GERA CSV E ENVIA PARA AZURE BLOB ===
data_hoje_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
NOME_ARQUIVO_CSV = f"{data_hoje_str}_resumo_anuncios.csv"
CAMINHO_BLOB = f"{CAMINHO_PASTA_BLOB}/{NOME_ARQUIVO_CSV}"

buffer = StringIO()
df.write_csv(buffer)
buffer.seek(0)

blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=CAMINHO_BLOB)
blob_client.upload_blob(buffer.getvalue(), overwrite=True)

print(f"✅ CSV de keywords enviado com sucesso: {CONTAINER_NAME}/{CAMINHO_BLOB}")









# import gspread
# import polars as pl
# from oauth2client.service_account import ServiceAccountCredentials
# from azure.storage.blob import BlobServiceClient
# from io import StringIO
# from datetime import datetime
# import os
# from dotenv import load_dotenv

# # === CARREGAR VARIÁVEIS DE AMBIENTE ===
# load_dotenv()

# # === CREDENCIAIS AZURE ===
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# CONTAINER_NAME = "bronze"
# CAMINHO_PASTA_BLOB = "source_google/google_keywords"

# # === CONEXÃO COM AZURE BLOB STORAGE ===
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# # === CONFIGURAÇÕES GOOGLE SHEETS ===
# SHEET_ID = "1doaKLuEuJqu4PFte0lp9nr98MvVkZ1THy5-agzYMF1o"
# ABA_DADOS = "keywords"
# ABA_CONTROLE = "controle"

# # === AUTENTICAÇÃO GOOGLE SHEETS ===
# scope = [
#     "https://spreadsheets.google.com/feeds",
#     "https://www.googleapis.com/auth/drive"
# ]
# credentials = ServiceAccountCredentials.from_json_keyfile_name(
#     "etlaadssheets-130cef47c418.json", scope
# )
# gc = gspread.authorize(credentials)

# # === LEITURA DOS DADOS DA PLANILHA ===
# planilha = gc.open_by_key(SHEET_ID)
# aba = planilha.worksheet(ABA_DADOS)
# dados = aba.get_all_records()

# # === NORMALIZAÇÃO: tudo como string, vazio ou NaN vira "0" ===
# todas_as_colunas = set().union(*(linha.keys() for linha in dados))
# dados_normalizados = []
# for linha in dados:
#     nova_linha = {}
#     for col in todas_as_colunas:
#         valor = linha.get(col, "")
#         if str(valor).strip().lower() in ("", "none", "nan"):
#             nova_linha[col] = "0"
#         else:
#             nova_linha[col] = str(valor)
#     dados_normalizados.append(nova_linha)

# # === CONVERTE PARA DATAFRAME POLARS ===
# df = pl.DataFrame(dados_normalizados)

# # === OBTÉM A DATA MAIS RECENTE DA COLUNA "data" ===
# try:
#     data_max = df.select(pl.col("data")).max().item()
# except Exception as e:
#     print(f"❌ Erro ao extrair data mais recente: {e}")
#     data_max = datetime.now().strftime("%Y-%m-%d")

# # === ATUALIZA A PLANILHA DE CONTROLE ===
# controle = planilha.worksheet(ABA_CONTROLE)
# controle_dados = controle.get_all_values()

# for i, linha in enumerate(controle_dados):
#     if linha[0].strip().lower() == ABA_DADOS:
#         controle.update_cell(i + 1, 2, data_max)
#         break


# # === DATA E HORA PARA NOME DO ARQUIVO ===
# data_hoje_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# NOME_ARQUIVO_CSV = f"{data_hoje_str}_keywords.csv"
# CAMINHO_BLOB = f"{CAMINHO_PASTA_BLOB}/{NOME_ARQUIVO_CSV}"

# # === CONVERTE PARA CSV EM MEMÓRIA ===
# buffer = StringIO()
# df.write_csv(buffer)
# buffer.seek(0)

# # === ENVIA PARA O AZURE BLOB STORAGE ===
# blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=CAMINHO_BLOB)
# blob_client.upload_blob(buffer.getvalue(), overwrite=True)

# print(f"✅ Arquivo CSV enviado com sucesso para: {CONTAINER_NAME}/{CAMINHO_BLOB}")





# # import gspread
# # import polars as pl
# # from oauth2client.service_account import ServiceAccountCredentials
# # from azure.storage.blob import BlobServiceClient
# # from io import StringIO
# # from datetime import datetime
# # import os
# # from dotenv import load_dotenv

# # # === CARREGAR VARIÁVEIS DE AMBIENTE ===
# # load_dotenv()

# # # === CREDENCIAIS AZURE ===
# # STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# # STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# # CONTAINER_NAME = "bronze"
# # CAMINHO_PASTA_BLOB = "source_google/google_keywords"

# # # === CONEXÃO COM AZURE BLOB STORAGE ===
# # connection_string = (
# #     f"DefaultEndpointsProtocol=https;"
# #     f"AccountName={STORAGE_ACCOUNT_NAME};"
# #     f"AccountKey={STORAGE_ACCOUNT_KEY};"
# #     f"EndpointSuffix=core.windows.net"
# # )
# # blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# # # === CONFIGURAÇÕES GOOGLE SHEETS ===
# # SHEET_ID = "1doaKLuEuJqu4PFte0lp9nr98MvVkZ1THy5-agzYMF1o"
# # ABA = "keywords"

# # # === AUTENTICAÇÃO GOOGLE SHEETS ===
# # scope = [
# #     "https://spreadsheets.google.com/feeds",
# #     "https://www.googleapis.com/auth/drive"
# # ]
# # credentials = ServiceAccountCredentials.from_json_keyfile_name(
# #     "etlaadssheets-130cef47c418.json", scope
# # )
# # gc = gspread.authorize(credentials)

# # # === LEITURA DOS DADOS DA PLANILHA ===
# # planilha = gc.open_by_key(SHEET_ID)
# # aba = planilha.worksheet(ABA)
# # dados = aba.get_all_records()

# # # === NORMALIZAÇÃO: tudo como string, vazio ou NaN vira "0" ===
# # todas_as_colunas = set().union(*(linha.keys() for linha in dados))
# # dados_normalizados = []
# # for linha in dados:
# #     nova_linha = {}
# #     for col in todas_as_colunas:
# #         valor = linha.get(col, "")
# #         if str(valor).strip().lower() in ("", "none", "nan"):
# #             nova_linha[col] = "0"
# #         else:
# #             nova_linha[col] = str(valor)
# #     dados_normalizados.append(nova_linha)

# # # === CONVERTE PARA DATAFRAME POLARS ===
# # df = pl.DataFrame(dados_normalizados)

# # # === DATA E HORA PARA NOME DO ARQUIVO ===
# # data_hoje_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# # NOME_ARQUIVO_CSV = f"{data_hoje_str}_keywords.csv"
# # CAMINHO_BLOB = f"{CAMINHO_PASTA_BLOB}/{NOME_ARQUIVO_CSV}"

# # # === CONVERTE PARA CSV EM MEMÓRIA ===
# # buffer = StringIO()
# # df.write_csv(buffer)
# # buffer.seek(0)

# # # === ENVIA PARA O AZURE BLOB STORAGE ===
# # blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=CAMINHO_BLOB)
# # blob_client.upload_blob(buffer.getvalue(), overwrite=True)

# # print(f"✅ Arquivo CSV enviado com sucesso para: {CONTAINER_NAME}/{CAMINHO_BLOB}")
