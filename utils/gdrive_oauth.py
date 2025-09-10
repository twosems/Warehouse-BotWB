# utils/gdrive_oauth.py
from __future__ import annotations
import os
from typing import Optional, List
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def build_drive_oauth(client_secret_path: str, token_path: str):
    """
    Авторизация по OAuth (личный Google Drive).
    Сохраняет/читает токен из token_path.
    """
    creds: Optional[Credentials] = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
            # Откроет браузер на локальной машине; на сервере используем уже готовый token.json
            creds = flow.run_local_server(port=0)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds, cache_discovery=False)

def upload_file(drive, local_path: str, filename: str, folder_id: str) -> str:
    from googleapiclient.http import MediaFileUpload
    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)
    file = drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
    return file["id"]

def cleanup_old(drive, folder_id: str, keep_days: int, name_prefix: Optional[str] = None) -> int:
    """
    Удаляет файлы старше keep_days в указанной папке.
    Работает по дате модификации. Чтобы не усложнять — удаляем по имени-префиксу (если задан).
    """
    import datetime as dt

    # Запросим файлы из папки
    q_parts: List[str] = [f"'{folder_id}' in parents", "trashed = false"]
    if name_prefix:
        q_parts.append(f"name contains '{name_prefix}'")
    q = " and ".join(q_parts)

    files = []
    page_token = None
    while True:
        resp = drive.files().list(
            q=q,
            spaces="drive",
            fields="nextPageToken, files(id, name, modifiedTime)",
            pageToken=page_token,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    if keep_days <= 0:
        # Ничего не удаляем
        return 0

    threshold = dt.datetime.utcnow() - dt.timedelta(days=keep_days)
    threshold_iso = threshold.replace(microsecond=0).isoformat() + "Z"

    removed = 0
    for f in files:
        # modifiedTime в ISO, сравним строкой — доп. запрос не нужен
        if f.get("modifiedTime", "") < threshold_iso:
            try:
                drive.files().delete(fileId=f["id"]).execute()
                removed += 1
            except Exception:
                pass
    return removed
