"""
Este script é utilizado para gerar um token de refresh (OAUTH2.0) para ser utilizado em scripts que acessam a API do 
Google Drive. Normalmente, esse script só precisa ser executado uma vez para gerar o token de refresh, que ficará salvo 
em uma variável de ambiente para uso futuro. Se por algum motivo o token de refresh for perdido, este script pode ser
executado novamente para gerar um novo token.
"""

from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from os import getenv

load_dotenv()

flow = InstalledAppFlow.from_client_secrets_file(
    getenv("PATH_TO_CLIENT_SECRET"),
    "https://www.googleapis.com/auth/spreadsheets.readonly",
)
auth_uri = flow.authorization_url()
flow.run_local_server()
print("Refresh token:", flow.credentials.refresh_token, sep="\n")
