import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

def autenticar_youtube(
    client_secret_path="json_youtube.json",
    token_path="token_youtube.json"
):
    """
    Faz a autenticação com o YouTube e retorna as credenciais.
    Reutiliza o token salvo se existir.
    """
    SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as token_file:
            token_file.write(creds.to_json())

    return creds
