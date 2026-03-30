import re
import time
import os
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from datetime import datetime

load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://itvalleyschool.api-us1.com"  # ex: https://xxxx.api-us1.com
API_TOKEN = os.getenv("ACTIVECAMPAIGN_API_TOKEN", "")

if not API_TOKEN:
    raise ValueError("ACTIVECAMPAIGN_API_TOKEN environment variable not set")

# === AZURE CONFIGURATION ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "bronze"
CAMINHO_PASTA_BLOB = "source_activeCampaign/leads"

if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
    raise ValueError("STORAGE_ACCOUNT_NAME and STORAGE_ACCOUNT_KEY environment variables must be set")

HEADERS = {
    "Api-Token": API_TOKEN,
    "Accept": "application/json",
    "Content-Type": "application/json",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def _get(url: str, params: dict | None = None, retries: int = 5, backoff: float = 1.5) -> dict:
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=60)
            if r.status_code == 429:
                wait = backoff ** attempt
                logger.warning(f"Rate limit hit. Waiting {wait}s before retry (attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error on attempt {attempt + 1}/{retries}: {e}")
            if attempt == retries - 1:
                raise
            time.sleep(backoff ** attempt)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts")


def fetch_all(endpoint: str, key: str, limit: int = 100) -> list[dict]:
    """
    Busca todos os registros de um endpoint com paginação offset/limit.
    endpoint: '/api/3/contacts'
    key: 'contacts' (chave do JSON retornado)
    """
    url = f"{BASE_URL}{endpoint}"
    all_items = []
    offset = 0

    while True:
        data = _get(url, params={"limit": limit, "offset": offset})
        items = data.get(key, [])
        if not items:
            break
        all_items.extend(items)
        offset += limit

    return all_items


def slugify_col(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def main():
    try:
        logger.info("Iniciando extração de dados do ActiveCampaign...")
        
        # 1) Contatos (campos padrão)
        logger.info("Buscando contatos...")
        contacts = fetch_all("/api/3/contacts", "contacts", limit=100)
        df_contacts = pd.DataFrame(contacts)

        if df_contacts.empty:
            logger.error("Nenhum contato retornado.")
            return

        # Normaliza ID como string
        df_contacts["id"] = df_contacts["id"].astype(str)

        logger.info(f"Total de contatos: {len(df_contacts)}")
        
        # 2) Definições de campos personalizados
        logger.info("Buscando definições de campos personalizados...")
        fields = fetch_all("/api/3/fields", "fields", limit=100)
        df_fields = pd.DataFrame(fields)
        if not df_fields.empty:
            df_fields["id"] = df_fields["id"].astype(str)

            # Mapa fieldId -> nome do campo (slug)
            fieldid_to_name = {}
            for _, row in df_fields.iterrows():
                fid = str(row.get("id", "")).strip()
                title = str(row.get("title", "")).strip()
                if fid and title:
                    fieldid_to_name[fid] = f"cf__{slugify_col(title)}"
        else:
            fieldid_to_name = {}

        # 3) Valores dos campos personalizados (fieldValues)
        logger.info("Buscando valores de campos personalizados...")
        field_values = fetch_all("/api/3/fieldValues", "fieldValues", limit=100)
        df_fv = pd.DataFrame(field_values)

        if not df_fv.empty:
            # contact, field, value
            df_fv["contact"] = df_fv["contact"].astype(str)
            df_fv["field"] = df_fv["field"].astype(str)

            # Nome da coluna do campo
            df_fv["field_col"] = df_fv["field"].map(fieldid_to_name)
            # Se algum fieldId não tiver definição, cai num nome genérico
            df_fv["field_col"] = df_fv.apply(
                lambda r: r["field_col"] if isinstance(r["field_col"], str) and r["field_col"] else f"cf__field_{r['field']}",
                axis=1
            )

            # Pivot para ficar 1 linha por contato, 1 coluna por campo
            df_cf = (
                df_fv.pivot_table(
                    index="contact",
                    columns="field_col",
                    values="value",
                    aggfunc="first",
                )
                .reset_index()
                .rename(columns={"contact": "id"})
            )
        else:
            df_cf = pd.DataFrame(columns=["id"])

        # 4) Tags: contactTags (liga contato<->tag) e tags (dicionário)
        logger.info("Buscando tags de contatos...")
        contact_tags = fetch_all("/api/3/contactTags", "contactTags", limit=100)
        df_ct = pd.DataFrame(contact_tags)

        tags = fetch_all("/api/3/tags", "tags", limit=100)
        df_tags = pd.DataFrame(tags)

        if not df_ct.empty and not df_tags.empty:
            df_ct["contact"] = df_ct["contact"].astype(str)
            df_ct["tag"] = df_ct["tag"].astype(str)
            df_tags["id"] = df_tags["id"].astype(str)

            # Merge: df_ct.tag (ID) com df_tags.id (ID) para trazer o nome da tag
            df_ct = df_ct.merge(
                df_tags[["id", "tag"]].rename(columns={"id": "tag_id", "tag": "tag_name"}),
                left_on="tag",
                right_on="tag_id",
                how="left"
            )

            df_tags_agg = (
                df_ct.groupby("contact")["tag_name"]
                .apply(lambda s: sorted({str(x).strip() for x in s if str(x).strip() and str(x).strip() != "nan"}))
                .reset_index()
                .rename(columns={"contact": "id", "tag_name": "tags"})
            )
            df_tags_agg["tags"] = df_tags_agg["tags"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
        else:
            df_tags_agg = pd.DataFrame({"id": df_contacts["id"], "tags": ""}).drop_duplicates()

        # 5) Listas: contactLists (liga contato<->lista) e lists (dicionário)
        logger.info("Buscando listas de contatos...")
        contact_lists = fetch_all("/api/3/contactLists", "contactLists", limit=100)
        df_cl = pd.DataFrame(contact_lists)

        lists = fetch_all("/api/3/lists", "lists", limit=100)
        df_lists = pd.DataFrame(lists)

        if not df_cl.empty and not df_lists.empty:
            df_cl["contact"] = df_cl["contact"].astype(str)
            df_cl["list"] = df_cl["list"].astype(str)
            df_lists["id"] = df_lists["id"].astype(str)

            # merge para trazer nome da lista
            df_cl = df_cl.merge(df_lists[["id", "name"]].rename(columns={"id": "list"}), on="list", how="left")

            # status pode vir como "status" (1/2 etc.) ou strings dependendo do endpoint/conta
            # vamos montar um texto "Lista (status)"
            status_col = "status" if "status" in df_cl.columns else None

            def format_list(row):
                nm = str(row.get("name", "")).strip()
                if not nm or nm == "nan":
                    nm = "unknown_list"
                if status_col:
                    st = str(row.get(status_col, "")).strip()
                    if st and st != "nan":
                        return f"{nm} [{st}]"
                return nm

            df_cl["list_fmt"] = df_cl.apply(format_list, axis=1)

            df_lists_agg = (
                df_cl.groupby("contact")["list_fmt"]
                .apply(lambda s: sorted({str(x).strip() for x in s if str(x).strip() and str(x).strip() != "nan"}))
                .reset_index()
                .rename(columns={"contact": "id", "list_fmt": "lists"})
            )
            df_lists_agg["lists"] = df_lists_agg["lists"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
        else:
            df_lists_agg = pd.DataFrame({"id": df_contacts["id"], "lists": ""}).drop_duplicates()

        # 6) Junta tudo num dataset final (1 linha por contato)
        logger.info("Consolidando dados...")
        df_final = df_contacts.merge(df_cf, on="id", how="left")
        df_final = df_final.merge(df_tags_agg, on="id", how="left")
        df_final = df_final.merge(df_lists_agg, on="id", how="left")

        # 7) Limpeza opcional de colunas muito "internas"
        # Você pode comentar este bloco se quiser manter literalmente tudo que vier.
        drop_cols = [c for c in ["links"] if c in df_final.columns]
        if drop_cols:
            df_final = df_final.drop(columns=drop_cols)

        # 8) Fazer upload para Azure Blob Storage
        logger.info("Fazendo upload para Azure Blob Storage...")
        
        # Configurar conexão com Azure
        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={STORAGE_ACCOUNT_NAME};"
            f"AccountKey={STORAGE_ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        
        # Salvar CSV em memória e fazer upload
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"activecampaign_contatos_{timestamp}.csv"
        caminho_blob = f"{CAMINHO_PASTA_BLOB}/{nome_arquivo}"
        
        buffer = BytesIO()
        df_final.to_csv(buffer, index=False, encoding="utf-8-sig")
        buffer.seek(0)
        
        container_client.upload_blob(name=caminho_blob, data=buffer, overwrite=True)
        
        # Verificar se o arquivo foi criado
        try:
            blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=caminho_blob)
            properties = blob_client.get_blob_properties()
            logger.info(f"✓ Arquivo confirmado no Azure!")
            logger.info(f"  - Tamanho: {properties.size} bytes")
            logger.info(f"  - Last Modified: {properties.last_modified}")
        except Exception as e:
            logger.warning(f"Não foi possível confirmar arquivo: {e}")
        
        logger.info("✓ Extração concluída com sucesso!")
        logger.info(f"- Arquivo CSV enviado para: {caminho_blob}")
        logger.info(f"- Total de contatos: {len(df_final)}")
        logger.info(f"- Total de colunas: {len(df_final.columns)}")
        
    except Exception as e:
        logger.error(f"Erro durante execução: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

