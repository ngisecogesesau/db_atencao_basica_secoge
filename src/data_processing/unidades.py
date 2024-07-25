import os
import requests
from requests_ntlm import HttpNtlmAuth

# Recupera variáveis de ambiente
site_url = os.getenv('SHAREPOINT_SITE_URL')
username = os.getenv('SHAREPOINT_USERNAME')
password = os.getenv('SHAREPOINT_PASSWORD')

# Verifica se as variáveis foram carregadas corretamente
if not site_url or not username or not password:
    print("Erro: As variáveis de ambiente não estão configuradas corretamente.")
    exit(1)

# URL do arquivo
file_url = "https://prefeituradorecife.sharepoint.com/Shared%20Documents/SESAU/BI_Indicadores_Estrategicos/ANALISE_PEAB_USF_29.02_DemandaBI.xlsx"
download_path = "ANALISE_PEAB_USF_29.02_DemandaBI.xlsx"

# Faz a solicitação GET para o arquivo usando requests_ntlm
try:
    response = requests.get(file_url, auth=HttpNtlmAuth(username, password))

    # Verifica se a solicitação foi bem-sucedida
    if response.status_code == 200:
        with open(download_path, "wb") as local_file:
            local_file.write(response.content)
        print("Download concluído")
    else:
        print(f"Erro ao baixar o arquivo: {response.status_code} - {response.reason}")
        print(f"Resposta do Servidor: {response.text}")
except Exception as e:
    print(f"Exceção ocorreu: {e}")
