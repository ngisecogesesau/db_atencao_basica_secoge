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
logger = logging.getLogger(__name__)

def authenticate_to_sharepoint(site_url, username, password):
    """
    Authenticate to the SharePoint site.

    :param site_url: URL of the SharePoint site
    :param username: SharePoint username
    :param password: SharePoint password
    :return: ClientContext if authentication is successful, None otherwise
    """
    context_auth = AuthenticationContext(site_url)
    if context_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, context_auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        logger.info(f"Authenticated on site: {web.properties['Title']}")
        return ctx
    else:
        logger.error("Authentication failed")
        return None

def get_file_content(ctx, relative_url):
    """
    Get the content of a file from SharePoint.

    :param ctx: Authenticated ClientContext
    :param relative_url: The relative URL of the file in SharePoint
    :return: File content as bytes, None if error occurs
    """
    try:
        response = File.open_binary(ctx, relative_url)
        return BytesIO(response.content)
    except Exception as e:
        logger.error(f"Error getting file content: {e}")
        return None

def get_file_as_dataframes(relative_url, sheet_name=None, skiprows=0):
    """
    Obtém um arquivo do SharePoint como múltiplos DataFrames do pandas, um para cada planilha.

    :param relative_url: A URL relativa do arquivo no SharePoint
    :param sheet_name: Nome ou lista de nomes de planilhas para ler (padrão None lê todas as planilhas)
    :param skiprows: Número de linhas a pular no início
    :return: Um dicionário onde as chaves são nomes de planilhas e os valores são DataFrames
    """
    site_url = os.getenv('SHAREPOINT_SITE_URL')
    username = os.getenv('SHAREPOINT_USERNAME')
    password = os.getenv('SHAREPOINT_PASSWORD')

    ctx = authenticate_to_sharepoint(site_url, username, password)
    if not ctx:
        return None

    file_content = get_file_content(ctx, relative_url)
    if not file_content:
        return None

    try:
        dataframes = pd.read_excel(file_content, sheet_name=sheet_name, skiprows=skiprows)
        if isinstance(dataframes, dict):
            logger.info(f"Planilhas disponíveis: {list(dataframes.keys())}")
            return dataframes
        else:
            sheet_key = sheet_name if sheet_name else 'Sheet1'
            logger.info(f"Planilha lida: {sheet_key}")
            return {sheet_key: dataframes}
    except Exception as e:
        logger.error(f"Erro ao converter conteúdo para DataFrames: {e}")
        return None

def log_dataframes_info(dataframes):
    """
    Logs information about each DataFrame in the given dictionary and returns the dictionary.

    Parameters:
    dataframes (dict): A dictionary where the keys are sheet names and the values are DataFrames.

    Returns:
    dict: The input dictionary of DataFrames.
    """
    if dataframes is not None:
        logger.info("DataFrames criados com sucesso!")
        for sheet_name, df in dataframes.items():
            num_rows = df.shape[0]
            num_columns = df.shape[1]
            logger.info(f"\nDataFrame da aba '{sheet_name}':")
            logger.info(f"Número de linhas: {num_rows}")
            logger.info(f"Número de colunas: {num_columns}")
            logger.info(f"Primeiras linhas do DataFrame:\n{df.head()}")
    else:
        logger.error("Falha ao criar os DataFrames.")
    
    return dataframes

def create_dataframe_variables(dataframes, prefix):
    """
    Create dynamic variables for each DataFrame based on sheet names.

    Parameters:
    dataframes (dict): A dictionary where the keys are sheet names and the values are DataFrames.
    prefix (str): Prefix for the variable names.

    Returns:
    None
    """
    if dataframes:
        for sheet_name, df in dataframes.items():
            sanitized_sheet_name = sheet_name.replace(' ', '_').replace('-', '_')
            globals()[f'{prefix}_{sanitized_sheet_name}'] = df
            logger.info(f"Variável criada: {prefix}_{sanitized_sheet_name}")
