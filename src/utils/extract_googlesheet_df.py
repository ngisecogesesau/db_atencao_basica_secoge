from io import BytesIO
from turtle import down
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
import requests
from dotenv import load_dotenv
from os import getenv
import pandas as pd
import json
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]  # Todo fix


def authenticate(refresh_token, client_secret_file) -> str:
    """Gera credenciais de autenticação para APIs do Google a partir de um refresh token e demais dados do cliente."""
    with open(client_secret_file, "r") as f:
        secrets = json.loads(f.read())
        secrets = secrets["installed"]

    params = {
        "grant_type": "refresh_token",
        "client_id": secrets["client_id"],
        "client_secret": secrets["client_secret"],
        "refresh_token": refresh_token,
    }
    r = requests.post(secrets["token_uri"], data=params)
    if r.ok:
        return Credentials(r.json()["access_token"], scopes=SCOPES)
    else:
        raise Exception("Error refreshing token: Request failed")


def download_private_sheet(spreadsheet_id: str) -> BytesIO:
    """Retorna um objeto BytesIO com o conteúdo de uma planilha Google Sheets de acesso restrito."""

    credentials = authenticate(getenv("REFRESH_TOKEN"), getenv("PATH_TO_CLIENT_SECRET"))

    try:
        service = build("drive", "v3", credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to authenticate to Google API: {e}")
        return None

    result = service.files().export_media(
        fileId=spreadsheet_id,
        mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    file = BytesIO()
    downloader = MediaIoBaseDownload(file, result)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return file


def download_public_sheet(spreadsheet_id: str) -> BytesIO:
    """Retorna um objeto BytesIO com o conteúdo de uma planilha Google Sheets pública."""
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?id={spreadsheet_id}&format=xlsx"
    response = requests.get(url)
    if not response.ok:
        raise Exception("Error downloading public sheet")
    return BytesIO(response.content)


def get_file_as_dataframes_google(spreadsheet_id: str, skiprows=0) -> dict:
    """Retorna um dicionário de DataFrames a partir de uma planilha Google Sheets."""
    try:
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?id={spreadsheet_id}&format=xlsx"
        response = requests.get(url)
    except Exception as e:
        logger.error(f"Error retrieving request for spreadsheet {spreadsheet_id}: {e}")
        return None

    if not response.ok:
        logger.info(
            "Detected private Google spreadsheet. Authenticating and downloading..."
        )
        file = download_private_sheet(spreadsheet_id)
    else:
        logger.info("Detected public Google spreadsheet. Downloading...")
        file = download_public_sheet(spreadsheet_id)

    try:
        dataframes = pd.read_excel(
            file, engine="calamine", sheet_name=None, skiprows=skiprows
        )
        logger.info(f"Sheets available: {list(dataframes.keys())}")
        return dataframes
    except Exception as e:
        logger.error(f"Error converting content to DataFrames: {e}")
        return None
