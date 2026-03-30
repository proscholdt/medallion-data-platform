import requests
import sys
import json
import os
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/".join(__file__.split("\\")[:-1]))
from auth_token import get_hotmart_access_token
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# === CARREGAR VARIÁVEIS DE AMBIENTE ===
load_dotenv()

# === CONFIGURAÇÕES AZURE ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
FOLDER_NAME = "sourcer_mastertech"


if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env")

connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# === TOKEN HOTMART ===
token_data = get_hotmart_access_token()
access_token = token_data["access_token"]

# === ENDPOINT ===
BASE_URL = "https://developers.hotmart.com/payments/api/v1/sales/history"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {access_token}",
}

# === FILTRO DO PRODUTO ===
PRODUCT_ID = 5898132  # MasterTech: Machine Learning

# Mantém no valor que já funcionava no seu ambiente
MAX_RESULTS = 100

def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def request_sales(start_ms: int, end_ms: int, page_token: str | None = None) -> dict:
    params = {
        "max_results": MAX_RESULTS,
        "start_date": start_ms,
        "end_date": end_ms,
        "product_id": PRODUCT_ID,  # filtro principal
    }
    if page_token:
        params["page_token"] = page_token

    resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=60)

    print(f"REQUEST: {resp.url}")
    print(f"STATUS:  {resp.status_code}")

    if resp.status_code != 200:
        raise RuntimeError(f"Erro na requisição: {resp.status_code} | {resp.text}")

    return resp.json()

def fetch_window(start_dt: datetime, end_dt: datetime) -> list[dict]:
    start_ms = to_ms(start_dt)
    end_ms = to_ms(end_dt)

    all_items = []
    page = 1
    token = None

    while True:
        data = request_sales(start_ms, end_ms, token)

        items = data.get("items", []) or []
        page_info = data.get("page_info", {}) or {}
        token = page_info.get("next_page_token")

        all_items.extend(items)

        print(
            f"  PAGE {page} | veio={len(items)} | acumulado_janela={len(all_items)} "
            f"| next={'sim' if bool(token) else 'não'}"
        )

        if not token:
            break

        page += 1

    return all_items

# === RANGE TOTAL ===
inicio_total = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
fim_total = datetime.now(timezone.utc)

JANELA_DIAS = 30

print(f"Buscando vendas do produto {PRODUCT_ID} de {inicio_total.isoformat()} até {fim_total.isoformat()} (UTC)")
print(f"Modo: janelas de {JANELA_DIAS} dias + paginação por page_token\n")

items_total: list[dict] = []

cursor = inicio_total
janela_idx = 1

while cursor < fim_total:
    janela_inicio = cursor
    janela_fim = min(cursor + timedelta(days=JANELA_DIAS), fim_total)

    print(f"JANELA {janela_idx}: {janela_inicio.date()} -> {janela_fim.date()}")
    itens_janela = fetch_window(janela_inicio, janela_fim)
    items_total.extend(itens_janela)
    print(f"FIM JANELA {janela_idx}: +{len(itens_janela)} | total_acumulado={len(items_total)}\n")

    cursor = janela_fim
    janela_idx += 1

# === DEDUP (evita repetir venda em janelas adjacentes) ===
dedup = {}
sem_chave = []

for item in items_total:
    tx = (item.get("purchase") or {}).get("transaction")
    if tx:
        dedup[tx] = item
    else:
        sem_chave.append(item)

final_items = list(dedup.values()) + sem_chave

resultado = {
    "total_vendas": len(final_items),
    "filtros": {
        "product_id": PRODUCT_ID,
        "start_date_utc": inicio_total.isoformat(),
        "end_date_utc": fim_total.isoformat(),
        "janela_dias": JANELA_DIAS,
        "max_results": MAX_RESULTS,
    },
    "dados": final_items,
}

json_data = json.dumps(resultado, indent=2, ensure_ascii=False)

blob_name = f"{FOLDER_NAME}/hotmart_sales_history.json"
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
blob_client.upload_blob(json_data, overwrite=True)

print(f"Exportado para Azure Blob Storage: {CONTAINER_NAME}/{blob_name}")
print(f"Total de vendas (deduplicado): {len(final_items)}")
