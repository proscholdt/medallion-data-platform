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
FOLDER_NAME = "sourcer_mastertech"  # mesmo diretório das vendas

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

BASE_URL = "https://developers.hotmart.com/payments/api/v1/sales/users"

PRODUCT_ID = 5898132
MAX_RESULTS = 100
JANELA_DIAS = 30  # se der 400, teste 14 ou 7

def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def request_users(start_ms: int, end_ms: int, page_token: str | None = None) -> dict:
    params = {
        "start_date": start_ms,
        "end_date": end_ms,
        "product_id": PRODUCT_ID,
        "max_results": MAX_RESULTS,
    }
    if page_token:
        params["page_token"] = page_token

    resp = requests.get(
        BASE_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
        params=params,
        timeout=60,
    )

    print(f"REQUEST: {resp.url}")
    print(f"STATUS:  {resp.status_code}")

    if resp.status_code != 200:
        raise RuntimeError(f"Erro na requisição: {resp.status_code} | {resp.text}")

    return resp.json()

def fetch_window(start_dt: datetime, end_dt: datetime) -> list[dict]:
    start_ms = to_ms(start_dt)
    end_ms = to_ms(end_dt)

    items_all = []
    token = None
    page = 1

    while True:
        data = request_users(start_ms, end_ms, token)

        items = data.get("items", []) or []
        page_info = data.get("page_info", {}) or {}
        token = page_info.get("next_page_token")

        items_all.extend(items)

        print(
            f"  PAGE {page} | veio={len(items)} | acumulado_janela={len(items_all)} "
            f"| next={'sim' if bool(token) else 'não'}"
        )

        if not token:
            break

        page += 1

    return items_all

# === RANGE TOTAL (UTC) ===
inicio_total = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
fim_total = datetime.now(timezone.utc)

print(
    f"Buscando usuários do produto {PRODUCT_ID} "
    f"de {inicio_total.strftime('%d/%m/%Y')} até {fim_total.strftime('%d/%m/%Y %H:%M:%S')} (UTC)"
)
print(f"Modo: janelas de {JANELA_DIAS} dias + paginação page_token\n")

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

# === DEDUP (evita duplicar em janelas adjacentes) ===
dedup = {}
sem_chave = []

for item in items_total:
    user = item.get("user") or item
    user_id = user.get("id") if isinstance(user, dict) else None
    email = user.get("email") if isinstance(user, dict) else None
    role = item.get("role") or item.get("type") or ""

    if user_id:
        key = f"id:{user_id}|role:{role}"
        dedup[key] = item
    elif email:
        key = f"email:{email.lower()}|role:{role}"
        dedup[key] = item
    else:
        sem_chave.append(item)

final_items = list(dedup.values()) + sem_chave

resultado = {
    "total_usuarios": len(final_items),
    "filtros": {
        "product_id": PRODUCT_ID,
        "start_date_utc": inicio_total.isoformat(),
        "end_date_utc": fim_total.isoformat(),
        "janela_dias": JANELA_DIAS,
        "max_results": MAX_RESULTS,
    },
    "dados": final_items,
}

# === EXPORTAR LOCAL ===
# output_path = r"C:\Users\henrique\Desktop\Projetos-ITVALLEY\BI\BI\datamart\Hotmart_mastertech\hotmart_users.json"
# os.makedirs(os.path.dirname(output_path), exist_ok=True)

# with open(output_path, "w", encoding="utf-8") as f:
#     json.dump(resultado, f, indent=2, ensure_ascii=False)

# print(f"Dados exportados para (local): {output_path}")
# print(f"Total de usuários (deduplicado): {len(final_items)}")

# === UPLOAD PARA AZURE (mesmo diretório) ===
json_data = json.dumps(resultado, indent=2, ensure_ascii=False)

blob_name = f"{FOLDER_NAME}/hotmart_users.json"
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
blob_client.upload_blob(json_data, overwrite=True)

print(f"Dados exportados para (Azure): {CONTAINER_NAME}/{blob_name}")
