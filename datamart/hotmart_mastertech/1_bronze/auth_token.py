import requests


def get_hotmart_access_token() -> dict:
    # Variáveis fictícias direto no código
    client_id = "c4ba2df9-d325-4db9-85ea-ecc3d18632d3"
    client_secret = "0b9956a1-9bbf-4829-ac37-7b5fbe84b5a3"
    authorization_basic = "Basic YzRiYTJkZjktZDMyNS00ZGI5LTg1ZWEtZWNjM2QxODYzMmQzOjBiOTk1NmExLTliYmYtNDgyOS1hYzM3LTdiNWZiZTg0YjVhMw=="

    url = "https://api-sec-vlc.hotmart.com/security/oauth/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": authorization_basic,
    }

    # Body vazio, igual ao Java (RequestBody.create(mediaType, ""))
    resp = requests.post(url, params=params, headers=headers, data="", timeout=60)

    if resp.status_code >= 400:
        raise RuntimeError(f"Erro Hotmart OAuth: status={resp.status_code} body={resp.text[:2000]}")

    return resp.json()


if __name__ == "__main__":
    token_data = get_hotmart_access_token()
    print(token_data)
