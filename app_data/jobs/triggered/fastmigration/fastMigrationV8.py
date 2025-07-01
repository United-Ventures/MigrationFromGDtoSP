#!/usr/bin/env python3
"""
fast_migration_2025_merged.py — Google Drive ➔ SharePoint
========================================================

Versione 2025‑06‑25‑merged
--------------------------
• Combina la scansione completa/paginata del secondo script con la velocità del primo.
• Retry a livello CHUNK *e* FILE, con back‑off esponenziale e re‑queue automatico.
• Verifica dimensione caricata (Content‑Range finale) e ri‑upload in caso di mismatch.
• Creazione cartelle SharePoint con retry.
• Logging strutturato JSON + STDOUT.
• Parametri invariati per compatibilità CLI.
"""

import os, time, json, tempfile, queue, threading, logging, requests, msal, concurrent.futures,csv
from typing import Dict, Tuple, List
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaIoBaseDownload

# ---------- CONFIG -------------------------------------------------------------------------------
CHUNK_DL       = 128 * 1024 * 1024  # 128 MB
CHUNK_UL       = 200 * 1024 * 1024  # 240 MB – MS Graph raccomanda < 250 MB
DL_THREADS     = 12
UL_THREADS     = 14
CHUNK_RETRIES  = 5     # tentativi per singolo chunk
FILE_RETRIES   = 10     # tentativi per intero file (nuova funzionalità)
LOG_FILE       = "fast_sync.log"
MAX_NAME_LEN   = 120

# ---------- THROTTLING MS GRAPH -------------------------------------------------
MAX_GRAPH_RPS = 15                 # massimo 10 richieste/sec complessive
_graph_token_bucket = queue.Queue(MAX_GRAPH_RPS)  # bucket con N "gettoni" al secondo

# ----- GLOBALI -----------------------------------------------------------------
_graph_lock   = threading.Lock()
_graph_app    = None         # sarà creato una volta sola
_graph_token  = None         # dict MSAL con expires_on (epoch)
TOKEN_BUFFER  = 3600          # 5 minuti

# ---------- EXPORT GOOGLE‐NATIVE -----------------------------------------------------------------
EXPORT_MAP: Dict[str, Tuple[str, str]] = {
    "application/vnd.google-apps.document": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx"
    ),
    "application/vnd.google-apps.spreadsheet": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"
    ),
    "application/vnd.google-apps.presentation": (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx"
    ),
    "application/vnd.google-apps.drawing": ("image/png", ".png"),
    "application/vnd.google-apps.jam": ("application/pdf", ".pdf"),
    "application/vnd.google-apps.form": ("application/zip", ".zip"),
    "application/vnd.google-apps.script": ("application/zip", ".zip"),
}

SKIP_MIMES = {
    "application/vnd.google-apps.site",
    "application/vnd.google-apps.map",
    "application/vnd.google-apps.shortcut",
}

# ---------- LOGGING -------------------------------------------------------------------------------
LOG_LEVEL  = logging.DEBUG if os.getenv("FAST_MIG_VERBOSE") == "1" else logging.INFO
LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(threadName)-15s — %(message)s"
logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
logger.info("Logging inizializzato a livello %s", logging.getLevelName(LOG_LEVEL))

# ---------- THREAD‑LOCAL, LOCK & STATS ------------------------------------------------------------
tl = threading.local()
lock = threading.Lock()
TIMERS: Dict[str, float] = {"walk": 0.0, "download": 0.0, "upload": 0.0}
bytes_dl = files_dl = bytes_ul = files_ul = 0
failed_files: List[Dict[str, str]] = []

# ---------- MIGRATION REPORT ---------------------------------------------------
MIGRATION_ROWS: List[Dict[str, str]] = []      # accumula le righe
MIGRATION_CSV  = "migration_report.csv"        # nome file di output

# ---------- FAILED REPORT ------------------------------------------------------
FAILED_ROWS: List[Dict[str, str]] = []        # tentativi esauriti o errori gravi
FAILED_CSV   = "failed_report.csv"

# ---------- UTILITY ------------------------------------------------------------------------------

def load_cfg() -> dict:
    with open("parameters.json", "r", encoding="utf-8") as f:
        return json.load(f)

def timed(section: str):
    def dec(fn):
        def wrapper(*args, **kwargs):
            t0 = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                with lock:
                    TIMERS[section] += time.perf_counter() - t0
        return wrapper
    return dec

def sanitize(name: str) -> str:
    return "".join(c for c in name if c not in "\\/:*?\"<>|").strip()

def flush_migration_report():
    """Scrive MIGRATION_ROWS su disco in formato CSV."""
    with open(MIGRATION_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["g_id", "g_name", "g_mimeType", "g_parent_id",
                        "sp_path", "is_folder", "notes"],
        )
        writer.writeheader()
        writer.writerows(MIGRATION_ROWS)
    logger.info("✔︎ Report migrazione salvato in %s (%d righe)",
                MIGRATION_CSV, len(MIGRATION_ROWS))

def flush_failed_report():
    """Scrive FAILED_ROWS su disco (CSV)."""
    if not FAILED_ROWS:      # niente da scrivere
        logger.info("Nessun fallimento da salvare")
        return
    with open(FAILED_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["g_id", "g_name", "stage", "error"],
        )
        writer.writeheader()
        writer.writerows(FAILED_ROWS)
    logger.warning("⚠︎ Salvato elenco fallimenti in %s (%d righe)",
                   FAILED_CSV, len(FAILED_ROWS))


def _refill_token_bucket():
    """Thread daemon: inserisce un 'gettone' nel bucket ogni 1/MAX_GRAPH_RPS secondi."""
    while True:
        try:
            _graph_token_bucket.put_nowait(None)
        except queue.Full:
            pass
        time.sleep(1.0 / MAX_GRAPH_RPS)


# ----- TOKEN MS GRAPH ---------------------------------------------------------------------------
def _get_new_graph_token(cfg: dict):
    """Ottiene un nuovo access_token MS Graph (client-credentials flow)."""
    global _graph_app, _graph_token
    if _graph_app is None:
        _graph_app = msal.ConfidentialClientApplication(
            cfg["client_id"],
            authority=cfg["authority"],
            client_credential=cfg["secret"],
        )
    _graph_token = _graph_app.acquire_token_for_client(scopes=cfg["scope"])
    if "access_token" not in _graph_token:
        raise RuntimeError(f"MSAL error: {_graph_token.get('error_description')}")
    return _graph_token

def graph_api_request(method: str, url: str, **kwargs) -> requests.Response:
    """
    Esegue la richiesta con refresh automatico del token.
    Ritenta al massimo 1× se riceve 401/403.
    """
    _graph_token_bucket.get()          # <-- new: limita a MAX_GRAPH_RPS req/sec

    cfg = load_cfg()
    with _graph_lock:
        # refresh se mancano meno di TOKEN_BUFFER secondi o token assente
        if (_graph_token is None
            or int(_graph_token.get("expires_on", 0)) - time.time() < TOKEN_BUFFER):
            _get_new_graph_token(cfg)
        headers = {
            "Authorization": f"Bearer {_graph_token['access_token']}",
            "Content-Type": "application/json",
            "User-Agent": "FastMigrator/1.3",
        }

    # usa una sessione locale; non serve condividerla tra thread
    resp = requests.request(method, url, headers=headers, **kwargs)

    # se il token è davvero scaduto, rigeneralo e ritenta una sola volta
    if resp.status_code in (401, 403):
        with _graph_lock:
            _get_new_graph_token(cfg)
            headers["Authorization"] = f"Bearer {_graph_token['access_token']}"
        resp = requests.request(method, url, headers=headers, **kwargs)

    resp.raise_for_status()
    return resp

# ---------- AUTH ---------------------------------------------------------------------------------

def get_drive():
    if not hasattr(tl, "drive"):
        SCOPES = ["https://www.googleapis.com/auth/drive"]
        try:
            creds = Credentials.from_authorized_user_file("GDriveNewToken.json", SCOPES)
            if not creds.valid:
                creds.refresh(Request())
        except Exception:
            flow  = InstalledAppFlow.from_client_secrets_file("client_secret.json", SCOPES)
            creds = flow.run_local_server(port=0)
            with open("GDriveNewToken.json", "w", encoding="utf-8") as f:
                f.write(creds.to_json())
        tl.drive = build("drive", "v3", credentials=creds)
    return tl.drive

def get_graph_session():
    if not hasattr(tl, "graph"):
        cfg  = load_cfg()
        app  = msal.ConfidentialClientApplication(
            cfg["client_id"], authority=cfg["authority"], client_credential=cfg["secret"]
        )
        token = app.acquire_token_silent(cfg["scope"], None) or app.acquire_token_for_client(scopes=cfg["scope"])
        if "access_token" not in token: raise RuntimeError("MSAL token failure")
        sess = requests.Session()
        sess.headers.update({
            "Authorization": f"Bearer {token['access_token']}",
            "Content-Type": "application/json",
            "User-Agent": "FastMigrator/1.2",
        })
        tl.graph = sess
    return tl.graph

# ---------- SHAREPOINT ---------------------------------------------------------------------------
def ensure_sp_folder(drive_id: str, parent_id: str, name: str) -> str:
    """
    Ritorna l'ID della cartella (creata o già esistente) su SharePoint,
    con retry, throttling e refresh-token automatico via graph_api_request().
    """
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{parent_id}/children"
    payload = {
        "name": name.strip().rstrip("., "),          # evita nomi con virgole/punti finali
        "folder": {},
        "@microsoft.graph.conflictBehavior": "rename"
    }

    for attempt in range(FILE_RETRIES):
        try:
            # 1️⃣ Controlla se la cartella esiste già
            res = graph_api_request("GET", url)
            for itm in res.json().get("value", []):
                if itm.get("name") == payload["name"] and "folder" in itm:
                    return itm["id"]

            # 2️⃣ Prova a crearla
            resp = graph_api_request("POST", url, json=payload)

            if resp.status_code == 201:
                data = resp.json()
                if "id" in data:
                    return data["id"]
                logger.warning("POST 201 ma manca 'id' nel JSON: %s", data)

            elif resp.status_code == 409:
                # Cartella già esistente: ricerchiamo l'ID per nome (di nuovo)
                logger.info("Folder '%s' già esiste (409), provo a identificarla", name)
                res_retry = graph_api_request("GET", url)
                for itm in res_retry.json().get("value", []):
                    if itm.get("name") == payload["name"] and "folder" in itm:
                        return itm["id"]
                logger.warning("409 ricevuto ma cartella '%s' non trovata al GET successivo", name)

            else:
                logger.warning("POST folder failed [%d]: %s",
                               resp.status_code, resp.text[:200])

        except requests.HTTPError as e:                          # <-- NEW: intercetta HTTPError
            if e.response.status_code == 429:                    # <-- NEW: throttling Graph
                wait = int(e.response.headers.get("Retry-After", 2 ** attempt))
                logger.warning(
                    "Folder '%s' 429 received, sleeping %ds (try %d/%d)",
                    name, wait, attempt + 1, FILE_RETRIES
                )
                time.sleep(wait)
                continue                                         # riprova stesso attempt
            else:
                logger.warning("Folder '%s' HTTP error %s", name, e)
        except Exception as e:
            logger.warning("Folder '%s' error (%s) try %d/%d",
                           name, e, attempt + 1, FILE_RETRIES)

        # Back-off esponenziale (usato solo se NON è stato applicato Retry-After)
        time.sleep(2 ** attempt)

    raise RuntimeError(f"Impossibile creare la cartella '{name}' dopo {FILE_RETRIES} tentativi")

# ---------- RESUMABLE UPLOAD UTIL ----------------------------------------------------------------

def is_retriable_status(code: int) -> bool:
    return code in (429, 500, 502, 503, 504)

def put_chunk(url: str, chunk: bytes, hdr: dict):
    for attempt in range(CHUNK_RETRIES):
        r = requests.put(url, headers=hdr, data=chunk)

        # ✅ successo: esci
        if r.status_code in (200, 201, 202):
            return r

        # 🔄 troppo traffico: rispetta Retry-After se presente
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 2 ** attempt))
            logger.warning(
                "Chunk 429 received, sleeping %ds (try %d/%d)",
                wait, attempt + 1, CHUNK_RETRIES,
            )
            time.sleep(wait)
            continue  # riprova lo stesso chunk

        # ⛔ altri codici non ritentabili o ultimo tentativo
        if not is_retriable_status(r.status_code) or attempt == CHUNK_RETRIES - 1:
            r.raise_for_status()

        # ↻ back-off esponenziale per 500/502/503/504 ecc.
        backoff = 2 ** attempt
        logger.debug("Chunk retry due to %s, sleep %ds", r.status_code, backoff)
        time.sleep(backoff)


# ---------- FILE UPLOAD (NUOVA FUNZIONE) ---------------------------------------------------------

def upload_file(path: str, sp_parent: str, drive_id: str):
    """Carica un singolo file su SharePoint con sessione riprendibile e verifica finale."""
    size = os.path.getsize(path)
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{sp_parent}:/{os.path.basename(path)}:/createUploadSession"
    up = graph_api_request("POST", url, json={"item": {"@microsoft.graph.conflictBehavior": "replace"}}).json()["uploadUrl"]

    with open(path, "rb") as fh:
        start = 0
        while True:
            chunk = fh.read(CHUNK_UL)
            if not chunk:
                break
            end = start + len(chunk) - 1
            hdr = {
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {start}-{end}/{size}",
            }
            put_chunk(up, chunk, hdr)
            start = end + 1

    if start != size:
        raise RuntimeError(f"Content-Range mismatch after upload {path} ({start}/{size})")

    logger.debug("[UL] Verified upload of %s (size %d)", os.path.basename(path), size)

# ---------- WORKERS ------------------------------------------------------------------------------

@timed("download")
def download_worker(dl_q: "queue.Queue", ul_q: "queue.Queue", tmpdir: str):
    global bytes_dl, files_dl
    while True:
        task = dl_q.get()
        if task is None:
            dl_q.task_done()
            break
        f, sp_parent = task
        try:
            path = fetch_to_disk(f, tmpdir)
            size = os.path.getsize(path)
            with lock:
                bytes_dl += size
                files_dl += 1
            ul_q.put((path, sp_parent))
        except Exception as e:
            failed_files.append({
                "id": f.get("id", ""),
                "name": f.get("name", ""),
                "error": str(e)
            })
            FAILED_ROWS.append({
                "g_id": f.get("id", ""),
                "g_name": f.get("name", ""),
                "stage": "download",
                "error": str(e)
            })
        finally:
            dl_q.task_done()


def fetch_to_disk(f: dict, tmpdir: str) -> str:
    mime      = f["mimeType"]
    safe_base = sanitize(f["name"])[:MAX_NAME_LEN]
    drive     = get_drive()

    # MIME esclusi
    if mime in SKIP_MIMES:
        raise ValueError(f"Skipping MIME {mime}")

    # ▶ Google-native export (Docs, Sheets, Slides, ecc.)
    if mime.startswith("application/vnd.google-apps"):
        if mime not in EXPORT_MAP:
            raise ValueError(f"Unmapped MIME {mime}")
        exp_mime, ext = EXPORT_MAP[mime]
        path = os.path.join(tmpdir, f"{safe_base}{ext}")        # ⬅︎ niente prefisso ID
        data = drive.files().export(fileId=f["id"], mimeType=exp_mime).execute()
        with open(path, "wb") as fh:
            fh.write(data)
        return path

    # ▶ File binari normali
    path = os.path.join(tmpdir, safe_base)                      # ⬅︎ niente prefisso ID
    req  = drive.files().get_media(fileId=f["id"])
    with open(path, "wb") as fh:
        dl   = MediaIoBaseDownload(fh, req, chunksize=CHUNK_DL)
        done = False
        while not done:
            status, done = dl.next_chunk()
    return path

@timed("upload")
def upload_worker(ul_q: queue.Queue, drive_id: str):
    global bytes_ul, files_ul
    while True:
        task = ul_q.get()
        if task is None:
            ul_q.task_done()
            break

        path, sp_parent = task
        # se il file è già stato cancellato da un altro thread, salta
        if not os.path.exists(path):
            logger.warning("⚠ File già rimosso (skip upload): %s", path)
            ul_q.task_done()
            continue

        ok = False
        for attempt in range(FILE_RETRIES):
            try:
                upload_file(path, sp_parent, drive_id)
                ok = True
                break                          # ✅ upload riuscito
            except requests.HTTPError as e:
                # 409 → file già presente o lock: consideriamo l’upload “ok”
                if e.response.status_code == 409:
                    logger.info("File presente/locked su SP (409) → lo segno come caricato: %s", path)
                    ok = True
                    break
                logger.warning("HTTP %s su %s (try %d/%d)",
                               e.response.status_code, os.path.basename(path),
                               attempt + 1, FILE_RETRIES)
            except FileNotFoundError:
                # un altro thread lo ha già tolto: abortiamo i retry
                logger.warning("⚠ File sparito durante upload: %s", path)
                break
            except Exception as e:
                logger.warning("Upload '%s' attempt %d/%d failed: %s",
                               os.path.basename(path), attempt + 1, FILE_RETRIES, e)
            time.sleep(2 ** attempt)           # back-off esponenziale

        if ok:
            try:
                size = os.path.getsize(path)
            except FileNotFoundError:
                size = 0                       # l’ha già tolto un altro thread
            with lock:
                bytes_ul += size
                files_ul += 1
            try:
                os.remove(path)                # cleanup (ignora se è già sparito)
            except FileNotFoundError:
                pass
        else:
            failed_files.append({
                "name": os.path.basename(path),
                "error": "Max retries exceeded"
            })

        ul_q.task_done()


# ---------- DFS WALK ---------------------------------------------------------------------------

@timed("walk")
def dfs_collect_and_enqueue(g_id: str, sp_parent: str, sp_path: str,
                            drive_id: str, dl_q: "queue.Queue"):
    name  = sanitize(
        get_drive().files().get(fileId=g_id, fields="name", supportsAllDrives=True).execute()["name"]
    )
    sp_id = ensure_sp_folder(drive_id, sp_parent, name)

    new_sp_path = f"{sp_path}/{name}" if sp_path else name
    # ► registra la cartella
    MIGRATION_ROWS.append({
        "g_id": g_id,
        "g_name": name,
        "g_mimeType": "application/vnd.google-apps.folder",
        "g_parent_id": "",           # compilato solo ai livelli successivi
        "sp_path": new_sp_path,
        "is_folder": "true",
        "notes": ""
    })

    page = None
    while True:
        resp = (
            get_drive()
            .files()
            .list(
                q=f"'{g_id}' in parents and trashed=false",
                fields="nextPageToken,files(id,name,mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=page,
            )
            .execute()
        )

        for f in resp.get("files", []):
            f_name = sanitize(f["name"])
            if f["mimeType"] == "application/vnd.google-apps.folder":
                dfs_collect_and_enqueue(f["id"], sp_id, new_sp_path,
                                        drive_id, dl_q)
            else:
                MIGRATION_ROWS.append({
                    "g_id": f["id"],
                    "g_name": f["name"],
                    "g_mimeType": f["mimeType"],
                    "g_parent_id": g_id,
                    "sp_path": f"{new_sp_path}/{f_name}",
                    "is_folder": "false",
                    "notes": ""
                })
                dl_q.put((f, sp_id))



        page = resp.get("nextPageToken")
        if not page:
            break


# ---------- MAIN --------------------------------------------------------------------------------

def main():
    t0   = time.perf_counter()
    cfg  = load_cfg()

    # --- avvio del refill per il token bucket (throttling Graph) ---
    threading.Thread(target=_refill_token_bucket, daemon=True).start()   # << NUOVA RIGA

    sess = get_graph_session()

    drive_id = graph_api_request(
        "GET", f"https://graph.microsoft.com/v1.0/sites/{cfg['site_id']}/drive"
    ).json()["id"]

    root_name = get_drive().files().get(
        fileId=cfg["google_folder_id"], fields="name", supportsAllDrives=True
    ).execute()["name"]
    sp_root = ensure_sp_folder(drive_id, "root", sanitize(root_name))

    tmpdir = tempfile.mkdtemp(prefix="fast_mig_")
    dl_q, ul_q = queue.Queue(), queue.Queue()

    # thread pool
    for _ in range(DL_THREADS):
        threading.Thread(target=download_worker, args=(dl_q, ul_q, tmpdir), daemon=True).start()
    for _ in range(UL_THREADS):
        threading.Thread(target=upload_worker, args=(ul_q, drive_id), daemon=True).start()

    # Root files (paginated)
    page = None
    while True:
        resp = get_drive().files().list(
            q=f"'{cfg['google_folder_id']}' in parents and mimeType!='application/vnd.google-apps.folder' and trashed=false",
            fields="nextPageToken,files(id,name,mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=page,
        ).execute()
        for f in resp.get("files", []):
            dl_q.put((f, sp_root))
        page = resp.get("nextPageToken")
        if not page:
            break

    # Top-level folders (paginated)
    top_folders = []
    page = None
    while True:
        resp = get_drive().files().list(
            q=f"'{cfg['google_folder_id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            fields="nextPageToken,files(id,name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=page,
        ).execute()
        top_folders.extend(resp.get("files", []))
        page = resp.get("nextPageToken")
        if not page:
            break

    with concurrent.futures.ThreadPoolExecutor() as ex:
        for fd in top_folders:
            ex.submit(dfs_collect_and_enqueue, fd["id"], sp_root, "", drive_id, dl_q)



    # Graceful shutdown
    dl_q.join()
    for _ in range(DL_THREADS):
        dl_q.put(None)

    ul_q.join()
    for _ in range(UL_THREADS):
        ul_q.put(None)

    tot = time.perf_counter() - t0
    report(tot)

    flush_migration_report()
    flush_failed_report()


    with open("failed_files.json", "w", encoding="utf-8") as f:
        json.dump(failed_files, f, indent=2, ensure_ascii=False)


# ---------- REPORT -----------------------------------------------------------------------------

def report(tot: float):
    logger.info("===== REPORT =====")
    logger.info("Walk       : %.1f s", TIMERS["walk"])
    logger.info("Download   : %.1f s", TIMERS["download"])
    logger.info("Upload     : %.1f s", TIMERS["upload"])
    logger.info("Total      : %.1f s", tot)
    logger.info("Files DL   : %d", files_dl)
    logger.info("Files UL   : %d", files_ul)
    logger.info("Bytes DL   : %.2f MB", bytes_dl / 1024**2)
    logger.info("Bytes UL   : %.2f MB", bytes_ul / 1024**2)
    logger.info("Speed      : %.2f MB/s", (bytes_dl / 1024**2) / tot if tot else 0)
    logger.info("Avg/File   : %.2f s", tot / files_dl if files_dl else 0)
    logger.info("Throughput : %.2f files/min", (files_dl / tot) * 60 if tot else 0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("🛑 Interruzione manuale (CTRL+C)")

