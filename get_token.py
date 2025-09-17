# get_token.py — генерирует token.json для Google Drive (scope drive.file)
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

flow = InstalledAppFlow.from_client_secrets_file(
    "client_secret.json", SCOPES
)

# Вариант с локальным браузером (удобно на Windows):
creds = flow.run_local_server(port=0)

# Если вдруг браузер не открылся — поменяй строку выше на:
# creds = flow.run_console()

with open("token.json", "w", encoding="utf-8") as f:
    f.write(creds.to_json())

print("✅ token.json создан рядом со скриптом.")
