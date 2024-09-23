from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from os import getenv

load_dotenv()

flow = InstalledAppFlow.from_client_secrets_file(
    getenv("PATH_TO_CLIENT_SECRET"),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
)
auth_uri, _ = flow.authorization_url()
flow.run_local_server()
print("Refresh token:", flow.credentials.refresh_token, sep="\n")
