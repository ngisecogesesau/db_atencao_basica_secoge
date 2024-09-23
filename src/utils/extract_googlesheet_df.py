import google.oauth2.credentials
import requests
from dotenv import load_dotenv
from os import getenv
import pandas as pd
from io import BytesIO
import json
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def refresh_token(refresh_token, client_secret_file) -> str:
    """Retorna um novo access token a partir de um refresh token e demais dados do cliente."""
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
        return r.json()["access_token"]
    else:
        raise Exception("Error refreshing token: Request failed")

def get_file_as_dataframes_google(spreadsheet_id: str, skiprows=0) -> dict:
    """ """
    # Autenticação
    access_token = refresh_token(
        getenv("REFRESH_TOKEN"), getenv("PATH_TO_CLIENT_SECRET")
    )
    creds = google.oauth2.credentials.Credentials(access_token)

    try:
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?id={spreadsheet_id}&format=xlsx"
        response = requests.get(url, headers={"Authorization": "Bearer " + creds.token})
    except Exception as e:
        logger.error(f"Error retrieving request for spreadsheet {spreadsheet_id}: {e}")
        return None

    try:
        dataframes = pd.read_excel(
            BytesIO(response.content), sheet_name=None, skiprows=skiprows
        )
        logger.info(f"Sheets available: {list(dataframes.keys())}")
        return dataframes
    except Exception as e:
        logger.error(f"Error converting content to DataFrames: {e}")
        return None
