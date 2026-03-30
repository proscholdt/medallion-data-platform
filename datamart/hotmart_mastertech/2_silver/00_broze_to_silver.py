import os
import io
import json
import polars as pl
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# === CARREGAR VARIÁVEIS DE AMBIENTE ===
load_dotenv()

# === CONFIGURAÇÕES AZURE ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

CONTAINER_BRONZE = "bronze"
CONTAINER_SILVER = "silver"
FOLDER_NAME = "sourcer_mastertech"

ARQUIVO_SALES = "hotmart_sales_history.json"
ARQUIVO_USERS = "hotmart_users.json"

ARQUIVO_SALES_PARQUET = "hotmart_sales.parquet"
ARQUIVO_USERS_PARQUET = "hotmart_users.parquet"

if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env")

connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)


def baixar_json(container: str, folder: str, arquivo: str) -> dict:
    blob_path = f"{folder}/{arquivo}".replace("\\", "/")
    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_path)
    data_bytes = blob_client.download_blob().readall()
    return json.loads(data_bytes)


# =========================
# 1) LER SALES -> DF (COM HOTMART_FEE)
# =========================
payload_sales = baixar_json(CONTAINER_BRONZE, FOLDER_NAME, ARQUIVO_SALES)

dados_sales = payload_sales.get("dados", [])
if not isinstance(dados_sales, list):
    raise ValueError('A chave "dados" (sales) não é uma lista no JSON.')

rows_sales = []
for item in dados_sales:
    purchase = item.get("purchase", {}) or {}
    buyer = item.get("buyer", {}) or {}
    product = item.get("product", {}) or {}

    price = purchase.get("price", {}) or {}
    payment = purchase.get("payment", {}) or {}
    hotmart_fee = purchase.get("hotmart_fee", {}) or {}

    rows_sales.append(
        {
            "approved_date": purchase.get("approved_date"),
            "buyer_ucode": buyer.get("ucode"),
            "buyer_name": buyer.get("name"),
            "buyer_email": buyer.get("email"),
            "price_value": price.get("value"),
            "price_currency_code": price.get("currency_code"),
            "payment_method": payment.get("method"),
            "payment_type": payment.get("type"),
            "payment_installments_number": payment.get("installments_number"),
            "hotmart_fee_total": hotmart_fee.get("total"),
            "hotmart_fee_currency_code": hotmart_fee.get("currency_code"),
            "hotmart_fee_base": hotmart_fee.get("base"),
            "hotmart_fee_fixed": hotmart_fee.get("fixed"),
            "hotmart_fee_percentage": hotmart_fee.get("percentage"),
            "product_id": product.get("id"),
            "product_name": product.get("name"),
        }
    )

df_sales = pl.from_dicts(rows_sales).with_columns(
    pl.from_epoch("approved_date", time_unit="ms").alias("approved_datetime_utc")
)

# =========================
# 2) LER USERS -> DF (apenas ucode + document_value)
# =========================
payload_users = baixar_json(CONTAINER_BRONZE, FOLDER_NAME, ARQUIVO_USERS)

dados_users = payload_users.get("dados", [])
if not isinstance(dados_users, list):
    raise ValueError('A chave "dados" (users) não é uma lista no JSON.')

rows_users = []
for item in dados_users:
    users = item.get("users", []) or []
    if not isinstance(users, list):
        users = []

    for u in users:
        if u.get("role") != "BUYER":
            continue

        user = u.get("user", {}) or {}
        buyer_ucode = user.get("ucode")

        documents = user.get("documents", []) or []
        if not isinstance(documents, list):
            documents = []

        if documents:
            for d in documents:
                rows_users.append(
                    {
                        "buyer_ucode": buyer_ucode,
                        "buyer_name": user.get("name"),
                        "document_value": (d or {}).get("value"),
                    }
                )
        else:
            rows_users.append({"buyer_ucode": buyer_ucode, "buyer_name": user.get("name"), "document_value": None})

df_users = pl.from_dicts(rows_users)

# 1 documento por ucode (primeiro não-nulo)
df_users_agg = (
    df_users
    .group_by("buyer_ucode")
    .agg(
        pl.col("buyer_name").first().alias("buyer_name"),
        pl.col("document_value").drop_nulls().first().alias("document_value")
    )
)

# =========================
# 3) GERAR PARQUET DE SALES (SEM JOIN)
# =========================
print("DF SALES")
print(df_sales)
print("shape:", df_sales.shape)
print("columns:", df_sales.columns)

buffer_sales = io.BytesIO()
df_sales.write_parquet(buffer_sales)
buffer_sales.seek(0)

blob_sales_path = f"{FOLDER_NAME}/{ARQUIVO_SALES_PARQUET}".replace("\\", "/")
blob_client_sales_out = blob_service_client.get_blob_client(
    container=CONTAINER_SILVER,
    blob=blob_sales_path,
)
blob_client_sales_out.upload_blob(buffer_sales, overwrite=True)

print(f"OK: Parquet de SALES enviado para container '{CONTAINER_SILVER}' em '{blob_sales_path}'")

# =========================
# 4) GERAR PARQUET DE USERS (SEM JOIN)
# =========================
print("\nDF USERS (AGG)")
print(df_users_agg)
print("shape:", df_users_agg.shape)
print("columns:", df_users_agg.columns)

buffer_users = io.BytesIO()
df_users_agg.write_parquet(buffer_users)
buffer_users.seek(0)

blob_users_path = f"{FOLDER_NAME}/{ARQUIVO_USERS_PARQUET}".replace("\\", "/")
blob_client_users_out = blob_service_client.get_blob_client(
    container=CONTAINER_SILVER,
    blob=blob_users_path,
)
blob_client_users_out.upload_blob(buffer_users, overwrite=True)

print(f"OK: Parquet de USERS enviado para container '{CONTAINER_SILVER}' em '{blob_users_path}'")
