# ig_followers_auto_token.py
# Requisitos: pip install requests python-dotenv azure-storage-blob
import os, json, requests
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError

load_dotenv()

API_VER = "v23.0"
FB = f"https://graph.facebook.com/{API_VER}"

# ===== Configuração Instagram / Meta =====
def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            value = value.strip()
        if value:
            return value
    return None


APP_ID = _first_env(
    "ItsegItvalleyAPP",  # legado
    "FB_APP_ID",
    "META_APP_ID",
    "APP_ID",
) or "COLOQUE_SEU_APP_ID"

APP_SECRET = _first_env(
    "ItsegItvalleyAPP_SECRET",  # legado
    "FB_APP_SECRET",
    "META_APP_SECRET",
    "APP_SECRET",
) or "COLOQUE_SEU_APP_SECRET"
IG_USER_ID         = os.getenv("IG_USER_ID")
PAGINA_NOME        = os.getenv("PAGINA_NOME") or "ItValleySchool"
USER_TOKEN_INICIAL = os.getenv("USER_TOKEN") or None                  # só na 1ª execução
TOKEN_STORE        = os.getenv("TOKEN_STORE") or "tokens_ig.json"

# ===== Configuração Azure Blob =====
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY  = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME       = os.getenv("CONTAINER_NAME", "bronze")
# >>> salvar direto aqui, sem criar subpastas:
CAMINHO_DESTINO      = "source_redesociais/IT_instagram"


# === Debug: Print Azure config ===
print(f"STORAGE_ACCOUNT_NAME: {STORAGE_ACCOUNT_NAME}")
print(f"STORAGE_ACCOUNT_KEY: {'SET' if STORAGE_ACCOUNT_KEY else 'NOT SET'}")
print(f"CONTAINER_NAME: {CONTAINER_NAME}")

blob_service_client = None
container_client = None
if STORAGE_ACCOUNT_NAME and STORAGE_ACCOUNT_KEY:
    print("Azure credentials found. Attempting connection...")
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={STORAGE_ACCOUNT_NAME};"
        f"AccountKey={STORAGE_ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    try:
        container_client.create_container()
        print(f"Container '{CONTAINER_NAME}' created.")
    except ResourceExistsError:
        print(f"Container '{CONTAINER_NAME}' already exists.")
else:
    print("Azure credentials missing. Upload will be skipped.")

# ===== Renovação automática =====
REFRESH_BEFORE_DAYS = 10


def _has_app_creds() -> bool:
    return bool(
        APP_ID
        and APP_SECRET
        and APP_ID != "COLOQUE_SEU_APP_ID"
        and APP_SECRET != "COLOQUE_SEU_APP_SECRET"
    )


def _as_dict_error(err: object) -> dict | None:
    if isinstance(err, dict):
        return err
    if isinstance(err, BaseException):
        if getattr(err, "args", None) and isinstance(err.args[0], dict):
            return err.args[0]
    return None


def _is_token_expired_error(err: object) -> bool:
    data = _as_dict_error(err)
    if not data:
        return False
    e = data.get("error") or {}
    try:
        code = int(e.get("code"))
    except Exception:
        code = None
    try:
        subcode = int(e.get("error_subcode"))
    except Exception:
        subcode = None
    # 190 = OAuthException. 463/460 são comuns para token expirado/inválido.
    return code == 190 and subcode in {463, 460}

def req(url, **params):
    r = requests.get(url, params=params, timeout=30)
    try:
        j = r.json()
    except Exception:
        raise RuntimeError({"status": r.status_code, "text": r.text[:300]})
    if r.status_code != 200 or "error" in j:
        raise RuntimeError(j)
    return j

def app_access_token():
    if not _has_app_creds():
        raise RuntimeError(
            {
                "msg": "APP_ID/APP_SECRET não configurados. Defina FB_APP_ID/FB_APP_SECRET (ou ItsegItvalleyAPP/ItsegItvalleyAPP_SECRET) no .env para permitir debug/refresh de token.",
            }
        )
    return f"{APP_ID}|{APP_SECRET}"

def debug_token(token: str):
    j = req(f"{FB}/debug_token", input_token=token, access_token=app_access_token())
    data = j.get("data", {})
    is_valid = bool(data.get("is_valid", False))
    expires_at = data.get("expires_at")  # unix epoch (segundos) – pode ser None
    scopes = set(data.get("scopes", []))
    return is_valid, expires_at, scopes, data

def precisa_renovar(expires_at_unix: int) -> bool:
    if not expires_at_unix:
        return False
    exp = datetime.fromtimestamp(expires_at_unix, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    return (exp - now) <= timedelta(days=REFRESH_BEFORE_DAYS)

def exchange_user_token_to_long_lived(user_token: str) -> str:
    if not _has_app_creds():
        raise RuntimeError(
            {
                "msg": "Não consigo renovar token sem APP_ID/APP_SECRET. Defina FB_APP_ID/FB_APP_SECRET no .env.",
            }
        )
    j = req(f"{FB}/oauth/access_token",
            grant_type="fb_exchange_token",
            client_id=APP_ID,
            client_secret=APP_SECRET,
            fb_exchange_token=user_token)
    return j["access_token"]  # ~60 dias

def carregar_tokens():
    p = Path(TOKEN_STORE)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def salvar_tokens(store: dict):
    Path(TOKEN_STORE).write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")

def garantir_long_lived():
    store = carregar_tokens()
    long_user_token = store.get("long_user_token")

    if not long_user_token:
        if not USER_TOKEN_INICIAL:
            raise RuntimeError({"msg": "Primeira execução: defina USER_TOKEN (curto) com permissões e Página selecionada."})
        try:
            long_user_token = exchange_user_token_to_long_lived(USER_TOKEN_INICIAL)
        except Exception as e:
            if _is_token_expired_error(e):
                raise RuntimeError(
                    {
                        "msg": "USER_TOKEN expirou. Gere um novo USER_TOKEN (Graph API Explorer / fluxo OAuth) e rode novamente para criar tokens_ig.json.",
                        "detalhes": _as_dict_error(e) or str(e),
                    }
                )
            raise
        store["long_user_token"] = long_user_token
        salvar_tokens(store)
        return long_user_token

    if _has_app_creds():
        try:
            is_valid, expires_at, _scopes, _ = debug_token(long_user_token)
        except Exception as e:
            # Se o token já expirou, o debug_token pode falhar com OAuthException.
            if _is_token_expired_error(e):
                if USER_TOKEN_INICIAL:
                    # Tenta recriar um long-lived a partir de um user token novo.
                    try:
                        long_user_token = exchange_user_token_to_long_lived(USER_TOKEN_INICIAL)
                        store["long_user_token"] = long_user_token
                        salvar_tokens(store)
                        return long_user_token
                    except Exception as e2:
                        if _is_token_expired_error(e2):
                            raise RuntimeError(
                                {
                                    "msg": "Token expirou (long_user_token e USER_TOKEN). Gere um novo USER_TOKEN e rode novamente para atualizar tokens_ig.json.",
                                    "detalhes": _as_dict_error(e2) or str(e2),
                                }
                            )
                        raise
                raise RuntimeError(
                    {
                        "msg": "Token do Meta/Instagram expirou. Gere um novo USER_TOKEN (Graph API) e rode novamente para atualizar tokens_ig.json.",
                        "detalhes": _as_dict_error(e) or str(e),
                    }
                )
            # Se não foi erro de token expirado, segue usando o token existente.
            return long_user_token

        if (not is_valid) or precisa_renovar(expires_at):
            try:
                # Se estiver perto de expirar, tenta trocar por um novo long-lived.
                long_user_token = exchange_user_token_to_long_lived(long_user_token)
                store["long_user_token"] = long_user_token
                salvar_tokens(store)
            except Exception as e:
                # Se falhar a troca, tenta com USER_TOKEN (se existir).
                if USER_TOKEN_INICIAL:
                    long_user_token = exchange_user_token_to_long_lived(USER_TOKEN_INICIAL)
                    store["long_user_token"] = long_user_token
                    salvar_tokens(store)
                else:
                    if _is_token_expired_error(e):
                        raise RuntimeError(
                            {
                                "msg": "Token do Meta/Instagram expirou. Gere um novo USER_TOKEN (Graph API) e rode novamente.",
                                "detalhes": _as_dict_error(e) or str(e),
                            }
                        )
                    # Caso contrário, mantém o token atual.
                    return long_user_token

    return long_user_token

def get_followers(ig_user_id: str, token: str):
    j = req(f"{FB}/{ig_user_id}", fields="followers_count", access_token=token)
    return j.get("followers_count")

def upload_to_azure(payload: dict):
    """
    Envia o JSON para:
      bronze/IT_instagram/seguidores_itvalleyInstagram_YYYY-MM-DD.json
    """
    if not container_client:
        print("[DEBUG] Azure não configurado (sem STORAGE_ACCOUNT_NAME/KEY). Upload será ignorado.")
        return False, "Azure não configurado (sem STORAGE_ACCOUNT_NAME/KEY)."

    data_str = json.dumps(payload, ensure_ascii=False)
    today = date.today().isoformat()
    filename = f"seguidores_itvalleyInstagram_{today}.json"
    blob_path = f"{CAMINHO_DESTINO}/{filename}"
    print(f"[DEBUG] Tentando upload para blob: {blob_path}")

    content_settings = ContentSettings(content_type="application/json; charset=utf-8")
    try:
        container_client.upload_blob(
            name=blob_path,
            data=data_str.encode("utf-8"),
            overwrite=True,
            content_settings=content_settings
        )
        print(f"[DEBUG] Upload realizado com sucesso: {blob_path}")
        return True, blob_path
    except Exception as e:
        print(f"[DEBUG] Erro no upload: {e}")
        return False, str(e)

def main():
    try:
        long_user_token = garantir_long_lived()

        # Tenta com long-lived; se você ainda deixou USER_TOKEN_INICIAL, tenta como fallback
        try_order = [long_user_token] + ([USER_TOKEN_INICIAL] if USER_TOKEN_INICIAL else [])

        ultimo_erro = None
        needs_token_refresh = False
        for token in try_order:
            try:
                seguidores = get_followers(IG_USER_ID, token)
                payload = {
                    "pagina": PAGINA_NOME,
                    "data": date.today().isoformat(),
                    "seguidores": seguidores
                }
                print(json.dumps(payload, ensure_ascii=False))

                ok, info = upload_to_azure(payload)
                if ok:
                    print(json.dumps({"upload": "ok", "blob": info}, ensure_ascii=False))
                else:
                    print(json.dumps({"upload": "skip", "motivo": info}, ensure_ascii=False))
                return
            except Exception as e:
                ultimo_erro = getattr(e, "args", [str(e)])[0]
                if _is_token_expired_error(e):
                    needs_token_refresh = True

        # Se chegou aqui, não conseguiu com nenhum token
        payload = {
            "pagina": PAGINA_NOME,
            "data": date.today().isoformat(),
            "seguidores": None,
            "erro": "Token expirado" if needs_token_refresh else "Falha ao obter followers_count",
            "needs_token_refresh": needs_token_refresh,
            "detalhes": ultimo_erro
        }
        print(json.dumps(payload, ensure_ascii=False))
        try:
            upload_to_azure(payload)
        except Exception:
            pass

    except Exception as e:
        detalhes = getattr(e, "args", [str(e)])[0]
        is_expired = _is_token_expired_error(e)
        if isinstance(detalhes, dict) and ("msg" in detalhes) and ("expir" in str(detalhes.get("msg", "")).lower()):
            # Erros internos do nosso fluxo que já são acionáveis
            payload = {
                "pagina": PAGINA_NOME,
                "data": date.today().isoformat(),
                "seguidores": None,
                "erro": "Token expirado",
                "needs_token_refresh": True,
                "detalhes": detalhes,
            }
        else:
            payload = {
                "pagina": PAGINA_NOME,
                "data": date.today().isoformat(),
                "seguidores": None,
                "erro": "Token expirado" if is_expired else "Setup de token",
                "needs_token_refresh": bool(is_expired),
                "detalhes": detalhes,
            }
        print(json.dumps(payload, ensure_ascii=False))
        try:
            upload_to_azure(payload)
        except Exception:
            pass

if __name__ == "__main__":
    main()
