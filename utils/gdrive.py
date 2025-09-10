# utils/gdrive.py
from __future__ import annotations
import io
from datetime import datetime, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from typing import Optional

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

def build_drive(sa_json: dict):
    creds = service_account.Credentials.from_service_account_info(sa_json, scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def upload_file(drive, local_path: str, file_name: str, folder_id: str) -> str:
    file_metadata = {"name": file_name, "parents": [folder_id]}
    media = MediaIoBaseUpload(open(local_path, "rb"), mimetype="application/octet-stream", chunksize=1024*1024, resumable=True)
    file = drive.files().create(body=file_metadata, media_body=media, fields="id,name").execute()
    return file["id"]

def cleanup_old(drive, folder_id: str, older_than_days: int, name_prefix: Optional[str] = None) -> int:
    """Удалить файлы в папке старше N дней (по createdTime). Возвращает кол-во удалённых."""
    if older_than_days <= 0:
        return 0
    cutoff = (datetime.utcnow() - timedelta(days=older_than_days)).isoformat("T") + "Z"
    q = f"'{folder_id}' in parents and createdTime < '{cutoff}' and trashed = false"
    if name_prefix:
        q += f" and name contains '{name_prefix}'"
    page_token = None
    deleted = 0
    while True:
        resp = drive.files().list(q=q, spaces="drive", fields="nextPageToken, files(id,name)", pageToken=page_token).execute()
        for f in resp.get("files", []):
            drive.files().delete(fileId=f["id"]).execute()
            deleted += 1
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return deleted
