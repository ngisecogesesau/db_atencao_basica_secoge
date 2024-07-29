import os
import pandas as pd
from dotenv import load_dotenv
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from io import BytesIO
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)

def get_file_content(relative_url):
    """
    Get the content of a file from SharePoint.

    :param relative_url: The relative URL of the file in SharePoint
    :return: File content as bytes
    """
    site_url = os.getenv('SHAREPOINT_SITE_URL')
    username = os.getenv('SHAREPOINT_USERNAME')
    password = os.getenv('SHAREPOINT_PASSWORD')

    context_auth = AuthenticationContext(site_url)
    if context_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, context_auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()

        logging.info(f"Authenticated on site: {web.properties['Title']}")

        try:
            response = File.open_binary(ctx, relative_url)
            return BytesIO(response.content)
        except Exception as e:
            logging.error(f"Error getting file content: {e}")
            return None
    else:
        logging.error("Authentication failed")
        return None

def get_file_as_dataframes(relative_url):
    """
    Get a file from SharePoint as multiple pandas DataFrames, one for each sheet.

    :param relative_url: The relative URL of the file in SharePoint
    :return: A dictionary where keys are sheet names and values are DataFrames
    """
    file_content = get_file_content(relative_url)
    if file_content is not None:
        try:
            dataframes = pd.read_excel(file_content, sheet_name=None)
            logging.info(f"Sheets available: {list(dataframes.keys())}")
            return dataframes
        except Exception as e:
            logging.error(f"Error converting content to DataFrames: {e}")
            return None
    else:
        logging.error("Error getting file content.")
        return None
