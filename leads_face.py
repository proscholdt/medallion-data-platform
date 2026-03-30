# leads_facebook_all_in_one.py

import os
import time
import json
import requests
from typing import Dict, Any, List, Optional, Iterable
from dotenv import load_dotenv

load_dotenv()

# ===================== CONFIGURAÇÕES =====================

API_VERSION = "v23.0"

# Seu App e Página
APP_ID = os.getenv("FB_APP_ID")                    # wflow
APP_SECRET = os.getenv("FB_APP_SECRET")             # NUNCA commitar real em repositórios públicos
PAGE_ID = os.getenv("FB_PAGE_ID_ITVALLEY")          # It Valley School

# Cole aqui o User Access Token CURTO gerado no Graph API Explorer (1-2h)
USER_TOKEN_CURTO = os.getenv("USER_TOKEN")

# Filtros opcionais por data (YYYY-MM-DD). Deixe vazio para tudo.
LEADS_SINCE = ""    # ex: "2025-08-01"
LEADS_UNTIL = ""    # ex: "2025-08-31"

# Baixar nomes de campaign/adset/ad (batch). Deixe True para enriquecer.
ENRICH_NAMES = True

# Salvar Parquet com Polars (opcional)
USE_POLARS = True
try:
    import polars as pl
except Exception:
    USE_POLARS = False

# Saídas
CSV_PATH = "leads_meta.csv"
PARQUET_PATH = "leads_meta.parquet"

# ===================== HELPERS HTTP =====================

def gget(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """GET com retentativa simples e erros claros."""
    for i in range(3):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return {}
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1 + i)
            continue
        try:
            err = r.json()
        except Exception:
            err = {"text": r.text}
        raise RuntimeError(f"Graph GET error {r.status_code}: {err}")
    raise RuntimeError("Falha após retentativas GET.")

def paginated(url: str, params: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """Itera resultados paginados."""
    while True:
        data = gget(url, params)
        for item in data.get("data", []):
            yield item
        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        url, params = next_url, {}

# ===================== TROCAS DE TOKEN =====================

def exchange_for_long_user_token(app_id: str, app_secret: str, user_short_token: str) -> str:
    """Troca User Token curto por long-lived (~60 dias)."""
    url = f"https://graph.facebook.com/{API_VERSION}/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": user_short_token,
    }
    data = gget(url, params)
    access_token = data.get("access_token")
    if not access_token:
        raise RuntimeError(f"Não obtive long-lived user token: {data}")
    return access_token

def get_page_access_token(user_long_token: str, page_id: str) -> str:
    # 1) Quem é o usuário
    me = gget(f"https://graph.facebook.com/{API_VERSION}/me",
              {"access_token": user_long_token, "fields": "id,name"})
    print("User do token:", me)

    # 2) Scopes do user token
    debug = gget(f"https://graph.facebook.com/{API_VERSION}/debug_token", {
        "input_token": user_long_token,
        "access_token": f"{APP_ID}|{APP_SECRET}"
    })
    print("Scopes do token:", debug.get("data", {}).get("scopes"))

    # 3) O ÚNICO caminho suportado: /me/accounts (com access_token)
    acc = gget(f"https://graph.facebook.com/{API_VERSION}/me/accounts", {
        "access_token": user_long_token,
        "fields": "id,name,access_token,perms"
    })
    pages = acc.get("data", [])
    print("Páginas encontradas:", [{"id": p.get("id"), "name": p.get("name"), "tem_token": bool(p.get("access_token"))} for p in pages])

    for p in pages:
        if p.get("id") == page_id:
            page_token = p.get("access_token")
            if not page_token:
                raise RuntimeError(f"A página {page_id} veio SEM access_token. Perms: {p.get('perms')}")
            # 4) Validação: será que este page token acessa leadgen_forms?
            test = requests.get(
                f"https://graph.facebook.com/{API_VERSION}/{page_id}/leadgen_forms",
                params={"access_token": page_token, "limit": 1},
                timeout=30
            )
            if test.status_code != 200:
                try:
                    err = test.json()
                except Exception:
                    err = {"text": test.text}
                raise RuntimeError(f"Page token inválido p/ leadgen_forms: {err}")
            return page_token

    raise RuntimeError(
        "Sua página NÃO veio em /me/accounts. Gere NOVO USER TOKEN CURTO com:\n"
        "- pages_show_list, pages_read_engagement, pages_manage_metadata, ads_read, leads_retrieval\n"
        "- e MARQUE a página It Valley School no consent screen."
    )


# ===================== NORMALIZAÇÃO =====================

def normalize_field_data(field_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Converte field_data (lista) -> dict plano {campo: valor}.
    Também cria aliases buyer_full_name, buyer_email, buyer_phone.
    """
    out: Dict[str, Any] = {}
    for f in field_data or []:
        name = (f.get("name") or "").strip()
        vals = f.get("values") or []
        value = vals[0] if vals else None
        out[name] = value

        lname = name.lower()
        if lname in ("full_name", "name"):
            out.setdefault("buyer_full_name", value)
        elif lname in ("email", "email_address"):
            out.setdefault("buyer_email", value)
        elif lname in ("phone_number", "phone"):
            out.setdefault("buyer_phone", value)
    return out

# ===================== COLETA DE DADOS =====================

def list_forms(page_id: str, page_token: str) -> List[Dict[str, Any]]:
    url = f"https://graph.facebook.com/{API_VERSION}/{page_id}/leadgen_forms"
    params = {
        "fields": "id,name,status,leads_count,created_time",
        "limit": 100,
        "access_token": page_token,
    }
    return list(paginated(url, params))

def list_leads_from_form(form_id: str, page_token: str):
    url = f"https://graph.facebook.com/{API_VERSION}/{form_id}/leads"
    params = {
        "fields": "id,created_time,ad_id,adset_id,campaign_id,form_id,field_data",
        "limit": 200,
        "access_token": page_token,
    }
    if LEADS_SINCE:
        params["since"] = LEADS_SINCE
    if LEADS_UNTIL:
        params["until"] = LEADS_UNTIL
    yield from paginated(url, params)

# ===================== ENRIQUECIMENTO (BATCH) =====================

def chunked(iterable, size):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def fetch_names_batch(ids: List[str], page_token: str) -> Dict[str, Optional[str]]:
    """Multi-ID: GET /?ids=id1,id2&fields=name → {id: name}"""
    out: Dict[str, Optional[str]] = {}
    if not ids:
        return out
    for group in chunked(ids, 100):
        url = f"https://graph.facebook.com/{API_VERSION}/"
        params = {"ids": ",".join(group), "fields": "name", "access_token": page_token}
        data = gget(url, params)
        for k, v in (data or {}).items():
            out[k] = (v or {}).get("name")
        time.sleep(0.05)
    return out

def enrich_object_names_batch(rows: List[Dict[str, Any]], page_token: str) -> None:
    campaign_ids = {r.get("campaign_id") for r in rows if r.get("campaign_id")}
    adset_ids = {r.get("adset_id") for r in rows if r.get("adset_id")}
    ad_ids = {r.get("ad_id") for r in rows if r.get("ad_id")}

    cmap = fetch_names_batch(list(campaign_ids), page_token)
    amap = fetch_names_batch(list(adset_ids), page_token)
    admap = fetch_names_batch(list(ad_ids), page_token)

    for r in rows:
        cid = r.get("campaign_id")
        aid = r.get("adset_id")
        adid = r.get("ad_id")
        if cid and "campaign_name" not in r:
            r["campaign_name"] = cmap.get(cid)
        if aid and "adset_name" not in r:
            r["adset_name"] = amap.get(aid)
        # Se quiser também o nome do anúncio:
        # if adid and "ad_name" not in r:
        #     r["ad_name"] = admap.get(adid)

# ===================== PERSISTÊNCIA =====================

def save_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        print("Nenhum lead coletado.")
        return
    keys = sorted({k for row in rows for k in row.keys()})
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(keys) + "\n")
        for row in rows:
            vals = []
            for k in keys:
                v = row.get(k, "")
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, ensure_ascii=False)
                s = str(v).replace('"', '""')
                if ("," in s) or ("\n" in s) or ('"' in s):
                    s = f'"{s}"'
                vals.append(s)
            f.write(",".join(vals) + "\n")
    print(f"CSV salvo em: {path}")

def save_parquet(rows: List[Dict[str, Any]], path: str) -> None:
    if not USE_POLARS or not rows:
        return
    df = pl.DataFrame(rows)
    df.write_parquet(path)
    print(f"Parquet salvo em: {path}")

# ===================== MAIN =====================

def main():
    if not USER_TOKEN_CURTO or USER_TOKEN_CURTO.startswith("COLE_AQUI") or USER_TOKEN_CURTO.strip() == "":
        raise SystemExit("Cole o USER_TOKEN_CURTO gerado no Graph API Explorer na variável USER_TOKEN_CURTO.")

    print("Trocando User Token curto por long-lived (≈60 dias)...")
    user_long = exchange_for_long_user_token(APP_ID, APP_SECRET, USER_TOKEN_CURTO)

    print("Obtendo Page Access Token da página...")
    page_token = get_page_access_token(user_long, PAGE_ID)
    print("Page Access Token obtido com sucesso.")

    print("Listando formulários de lead...")
    forms = list_forms(PAGE_ID, page_token)
    print(f"Formulários encontrados: {len(forms)}")

    rows: List[Dict[str, Any]] = []
    for form in forms:
        form_id = form["id"]
        form_name = form.get("name")
        print(f"- Baixando leads de: {form_name} ({form_id})")
        for lead in list_leads_from_form(form_id, page_token):
            base = {
                "lead_id": lead.get("id"),
                "created_time": lead.get("created_time"),
                "form_id": lead.get("form_id"),
                "form_name": form_name,
                "ad_id": lead.get("ad_id"),
                "adset_id": lead.get("adset_id"),
                "campaign_id": lead.get("campaign_id"),
            }
            buyer = normalize_field_data(lead.get("field_data"))
            rows.append({**base, **buyer})

    if rows and ENRICH_NAMES:
        print("Enriquecendo nomes de campaign/adset/anúncio (batch)...")
        enrich_object_names_batch(rows, page_token)

    print(f"Total de leads coletados: {len(rows)}")
    save_csv(rows, CSV_PATH)
    save_parquet(rows, PARQUET_PATH)
    print("Concluído.")

if __name__ == "__main__":
    main()
