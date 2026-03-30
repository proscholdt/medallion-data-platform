import os
import logging
import requests
import json
from dotenv import load_dotenv
import time

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://itvalleyschool.api-us1.com"
API_TOKEN = os.getenv("ACTIVECAMPAIGN_API_TOKEN", "")

if not API_TOKEN:
    raise ValueError("ACTIVECAMPAIGN_API_TOKEN environment variable not set")

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
                logger.warning(f"Rate limit. Aguardando {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(backoff ** attempt)


def main():
    try:
        logger.info("=" * 100)
        logger.info("EXTRAÇÃO DE DADOS DE UM CLIENTE")
        logger.info("=" * 100)
        
        # 1) Buscar primeiro contato
        logger.info("\n📌 Buscando um contato...")
        url = f"{BASE_URL}/api/3/contacts"
        data = _get(url, params={"limit": 1, "offset": 0})
        contacts = data.get("contacts", [])
        
        if not contacts:
            logger.error("Nenhum contato encontrado!")
            return
        
        contact = contacts[0]
        contact_id = contact.get("id")
        
        print("\n" + "=" * 100)
        print("CAMPOS PADRÃO DO CONTATO")
        print("=" * 100)
        
        for key, value in contact.items():
            if key not in ["links", "accountContacts", "scoreValues"]:  # Pular campos muito grandes
                print(f"  {key:<35} = {value}")
        
        # 2) Buscar campos personalizados
        logger.info("\n📌 Buscando definição de campos personalizados...")
        fields_data = _get(f"{BASE_URL}/api/3/fields", params={"limit": 100, "offset": 0})
        fields = fields_data.get("fields", [])
        
        # Criar mapa ID -> Título
        field_map = {str(f.get("id")): f.get("title") for f in fields}
        
        logger.info(f"   Total de campos: {len(fields)}")
        
        # 3) Buscar valores de campos personalizados para este contato
        logger.info(f"\n📌 Buscando valores de campos personalizados para contato {contact_id}...")
        fv_data = _get(f"{BASE_URL}/api/3/fieldValues", params={"limit": 100, "offset": 0})
        field_values = fv_data.get("fieldValues", [])
        
        # Filtrar para este contato
        contact_field_values = [fv for fv in field_values if str(fv.get("contact")) == str(contact_id)]
        
        print("\n" + "=" * 100)
        print("CAMPOS PERSONALIZADOS DO CONTATO")
        print("=" * 100)
        
        if contact_field_values:
            for fv in contact_field_values:
                field_id = fv.get("field")
                field_title = field_map.get(str(field_id), f"DESCONHECIDO (ID:{field_id})")
                value = fv.get("value")
                print(f"  {field_title:<35} = {value}")
        else:
            print("  ⚠️  Este contato não tem campos personalizados preenchidos")
        
        # 4) Resumo
        print("\n" + "=" * 100)
        print("RESUMO")
        print("=" * 100)
        print(f"  ID Contato: {contact_id}")
        print(f"  Email: {contact.get('email')}")
        print(f"  Nome: {contact.get('firstName')} {contact.get('lastName')}")
        print(f"  Data de Criação: {contact.get('cdate')}")
        print(f"  Data de Atualização: {contact.get('updated_timestamp')}")
        print(f"  Total de Campos Padrão: {len([k for k in contact.keys() if k not in ['links', 'accountContacts', 'scoreValues']])}")
        print(f"  Total de Campos Personalizados Preenchidos: {len(contact_field_values)}")
        print("=" * 100)
        
    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
