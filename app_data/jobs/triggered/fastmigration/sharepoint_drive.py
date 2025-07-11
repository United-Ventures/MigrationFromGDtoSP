"""
sharepoint_drive.py  – v3.8 (SID = webUrl sul record Company)
============================================================
Estende la v3.7 aggiungendo il salvataggio automatico del **campo SID**
(su Company) con il link SharePoint (webUrl) della cartella principale
/Accounts/<Account_Name>.

Flusso riassunto
---------------
1. Da uno *Deal ID* → recupera il riferimento Company.
2. Se il record Company non ha ancora **SID** → crea la cartella
   “/Accounts/<Company>” e scrive nel campo SID il relativo *webUrl*.
3. Garantisce le 9 sottocartelle standard, la folder “01. Presentation…”,
   e la sub‑cartella datata del Deal.
4. **Non** scrive più nulla sul Deal; restituisce soltanto il link alla
   cartella del giorno..

Parametri aggiunti (costruttore `SharePointDrive`)
--------------------------------------------------
- `company_sid_field` (str)  → nome del campo su **Accounts** dove
  scrivere il link SharePoint principale.  Default: "sid".

Uso invarato lato Flask / Zoho.
"""
from __future__ import annotations
import os, json, pathlib, threading, queue, time, re, urllib.parse
from datetime import date
from typing import Optional
import logging

import requests, msal
from dotenv import load_dotenv

# ─── ZOHO ──────────────────────────────────────────────────────────────────────
import ZInterface as ZOHO
get_record = ZOHO.getCompleteRecord
set_field  = ZOHO.SetRecordField

# ─── CONFIG ────────────────────────────────────────────────────────────────────
load_dotenv()
PARAM_FILE = "parameters.json"
CFG: dict[str, str] = {}
if pathlib.Path(PARAM_FILE).exists():
    CFG = json.loads(pathlib.Path(PARAM_FILE).read_text("utf-8"))
for k in ["tenant_id", "client_id", "secret", "domain", "site_path", "library"]:
    CFG.setdefault(k, os.getenv(f"MS_{k.upper()}") or os.getenv(k.upper()))
missing = [k for k in ["tenant_id", "client_id", "secret", "domain", "site_path", "library"] if not CFG.get(k)]
if missing:
    raise RuntimeError(f"Parametri mancanti: {', '.join(missing)}")

# ─── MSAL + throttling ─────────────────────────────────────────────────────────
_AUTH   = f"https://login.microsoftonline.com/{CFG['tenant_id']}"
_SCOPE  = ["https://graph.microsoft.com/.default"]
_APP: Optional[msal.ConfidentialClientApplication] = None
_TOKEN: dict = {}
_TLOCK = threading.Lock()
RPS = 8
_BUCKET = queue.Queue(RPS)

def _bucket():
    while True:
        try:
            _BUCKET.put_nowait(None)
        except queue.Full:
            pass
        time.sleep(1 / RPS)
threading.Thread(target=_bucket, daemon=True).start()

def _token() -> str:
    global _APP, _TOKEN
    with _TLOCK:
        if _APP is None:
            _APP = msal.ConfidentialClientApplication(
                CFG["client_id"], authority=_AUTH, client_credential=CFG["secret"]
            )
        if not _TOKEN or int(_TOKEN.get("expires_on", 0)) - time.time() < 45:
            _TOKEN = _APP.acquire_token_for_client(scopes=_SCOPE)
            if "access_token" not in _TOKEN:
                raise RuntimeError(_TOKEN.get("error_description"))
        return _TOKEN["access_token"]

def _greq(method: str, url: str, *, json_body: dict | None = None) -> requests.Response:
    _BUCKET.get()
    hdr = {"Authorization": f"Bearer {_token()}"}
    if json_body is not None:
        hdr["Content-Type"] = "application/json"
    r = requests.request(method, url, headers=hdr, json=json_body)
    if r.status_code in (401, 403):
        hdr["Authorization"] = f"Bearer {_token()}"; r = requests.request(method, url, headers=hdr, json=json_body)
    r.raise_for_status(); return r

# ─── Site & Drive ID cache ────────────────────────────────────────────────────
_SITE_ID: Optional[str]  = CFG.get("site_id")
_DRIVE_ID: Optional[str] = CFG.get("drive_id")

def _ensure_ids():
    global _SITE_ID, _DRIVE_ID
    if _SITE_ID and _DRIVE_ID: return
    if not _SITE_ID:
        _SITE_ID = _greq("GET", f"https://graph.microsoft.com/v1.0/sites/{CFG['domain']}:{CFG['site_path']}").json()["id"]
        CFG["site_id"] = _SITE_ID
    drives = _greq("GET", f"https://graph.microsoft.com/v1.0/sites/{_SITE_ID}/drives").json()["value"]
    _DRIVE_ID = next((d["id"] for d in drives if d["name"].lower()==CFG["library"].lower()), None)
    if not _DRIVE_ID: raise RuntimeError(f"Libreria '{CFG['library']}' non trovata")
    CFG["drive_id"] = _DRIVE_ID
    pathlib.Path(PARAM_FILE).write_text(json.dumps(CFG, indent=2))

# ─── Helper cartelle & link ───────────────────────────────────────────────────
_ILLEGAL = re.compile(r'[<>:"/\\|?*]')
_pre_made = [
    "01. Presentation deck e business plan",
    "02. Investment memorandum",
    "03. Rounds",
    "04. Board of Directors / Shareholders Meetings / Committee",
    "05. Monitoring and business review",
    "06. Bilanci",
    "07. Visure, libro soci",
    "08. Press Release and Logos",
    "09. Other",
]
_PREMADE = [_ILLEGAL.sub("‑", n).strip() for n in _pre_made]

def _safe(n:str)->str: return _ILLEGAL.sub("‑", n).strip()

def _enc(path:str)->str: return "/".join(urllib.parse.quote(p, safe=" ") for p in path.split("/"))

def _ensure_folder(path:str)->str:
    _ensure_ids(); clean="/".join(_safe(s) for s in path.strip("/").split("/")); enc=_enc(clean)
    # prova GET
    try:
        meta=_greq("GET", f"https://graph.microsoft.com/v1.0/drives/{_DRIVE_ID}/root:/{enc}").json()
        if "id" in meta: return meta["id"]
    except requests.HTTPError as e:
        if e.response.status_code not in (400,404): raise
    # crea step‑by‑step
    seg=clean.split("/"); base=f"https://graph.microsoft.com/v1.0/drives/{_DRIVE_ID}/root:/"
    for i,s in enumerate(seg):
        sub="/".join(seg[:i+1])
        try:_greq("GET", base+_enc(sub))
        except requests.HTTPError as e:
            if e.response.status_code in (400,404):
                par="/".join(seg[:i]); par_api=base+(_enc(par)+":" if par else ":")
                _greq("POST", par_api+"/children", json_body={"name":s,"folder":{},"@microsoft.graph.conflictBehavior":"rename"})
            else: raise
    return _greq("GET", base+_enc(clean)).json()["id"]

def _link(item_id:str)->str:
    _ensure_ids(); return _greq("GET", f"https://graph.microsoft.com/v1.0/drives/{_DRIVE_ID}/items/{item_id}?select=webUrl").json()["webUrl"]

# ─── Classe principale ───────────────────────────────────────────────────────
class SharePointDrive:
    _FALLBACK = ["Company","Account_Name","Account","Company_Name","Company_Name__c"]

    def __init__(
            self, 
            module="Deals", 
            gid_field="sid", 
            parent_anchor=None, 
            parent_module="Accounts",
            parent_folder_name="01. Presentation deck e business plan", 
            parent_gid_field="gdriveextension__Drive_Folder_ID",
            company_sid_field="sid", 
            deal_folder_name="Deal"):

            self.module=module; 
            self.gid_field=gid_field; 
            self.parent_anchor=parent_anchor; 
            self.parent_module=parent_module
            self.parent_folder_name=parent_folder_name; 
            self.parent_gid_field=parent_gid_field
            self.company_sid_field=company_sid_field; 
            self.deal_folder_name=deal_folder_name

    def buttonZohoDeals(self, deal_id:int)->str:
        deal=get_record(self.module, deal_id); comp_ref=self._find_company(deal)
        if comp_ref is None: raise RuntimeError("Deal senza Company collegata")
        comp_id=comp_ref.get_id(); company=get_record(self.parent_module, comp_id)
        acc_name=company.get_key_value("Account_Name") or "Company"

        # Cartella Company
        comp_path=f"/Accounts/{_safe(acc_name)}"; comp_id_sp=_ensure_folder(comp_path)
        # Scrivi link nel campo SID della Company se mancante
        if not company.get_key_value(self.company_sid_field):
            set_field(self.parent_module, comp_id, self.company_sid_field, _link(comp_id_sp))
        # assicura pre‑made
        for sub in _PREMADE: _ensure_folder(f"{comp_path}/{sub}")
        pres_path=f"{comp_path}/{_safe(self.parent_folder_name)}"; _ensure_folder(pres_path)
        deal_path=f"{pres_path}/{date.today():%Y %m %d} {_safe(self.deal_folder_name)}"
        deal_id_sp=_ensure_folder(deal_path)
        return _link(deal_id_sp)

    def _find_company(self, deal):
        anchors=[self.parent_anchor] if self.parent_anchor else []
        anchors+=[a for a in self._FALLBACK if a and a not in anchors]
        for f in anchors:
            ref=deal.get_key_value(f) if f else None
            if ref is not None:
                self.parent_anchor=f; return ref
        return None

    def buttonZohoBusinessReview(self, deal_id: int, module="UV_Business_Reviews", parent_folder_name="05. Monitoring and business review") -> str:
        if module:
            self.module = module
        if parent_folder_name:
            self.parent_folder_name = parent_folder_name
        # 1. Recupera Deal e Company
        deal = get_record(self.module, deal_id)
        with open("module_debug.txt", "w", encoding="utf-8") as f:
            f.write(f"self.module = {self.module}\n")

        comp_ref = self._find_company(deal)
        if comp_ref is None:
            raise RuntimeError("Business Review senza Company collegata")

        comp_id = comp_ref.get_id()
        company = get_record(self.parent_module, comp_id)
        if not company:
            raise RuntimeError(f"Company con ID {comp_id} non trovata")

        acc_name = company.get_key_value("Account_Name") or "Company"

        # 2. Cartella Company
        comp_path = f"/Accounts/{_safe(acc_name)}"
        _ensure_folder(comp_path)

        # 3. Cartella “05. Monitoring and business review”
        mon_root = f"{comp_path}/{_safe(self.parent_folder_name)}"
        _ensure_folder(mon_root)

        # 4. Sottocartella datata: ‘YYYY MM DD Business Review’
        dated_folder = f"{mon_root}/{date.today():%Y %m %d} Business Review"
        folder_id = _ensure_folder(dated_folder)
        link = _link(folder_id)

        # 5. Scrivi il link nel campo SID se vuoto
        if not company.get_key_value(self.company_sid_field):
            set_field(self.parent_module, comp_id, self.company_sid_field, link)

        return link

    def _find_company(self, deal):
        """
        Trova il riferimento all'Account (Company) collegato al Deal.
        Per il modulo 'Round', il campo è 'Startup'.
        """
        return deal.get_key_value("Startup")

    def buttonZohoRound(self, deal_id: int, module="Round", parent_folder_name="03. Rounds") -> str:
        # Override temporaneo dei parametri (non persistenti sull'oggetto)
        module = module or self.module
        parent_folder_name = parent_folder_name or self.parent_folder_name

        # 1. Recupera il Deal
        deal = get_record(module, deal_id)

        # 2. Trova la Company (Startup)
        comp_ref = self._find_company(deal)

        # DEBUG completo su file
        with open("module_debug.txt", "w", encoding="utf-8") as f:
            f.write("==== DEBUG ZOHO ROUND ====\n")
            f.write(f"module = {module}\n")
            f.write(f"deal_id = {deal_id}\n")
            f.write(f"deal object = {deal}\n")
            f.write(f"comp_ref = {comp_ref}\n")
            try:
                f.write(f"comp_ref ID = {comp_ref.get_id()}\n")
            except Exception as e:
                f.write(f"Errore su comp_ref.get_id(): {e}\n")
            f.write("\n--- deal.get_key_values() ---\n")
            json.dump(deal.get_key_values(), f, indent=2, default=str)

        if comp_ref is None:
            raise RuntimeError("Round senza Company collegata")

        comp_id = comp_ref.get_id()
        company = get_record(self.parent_module, comp_id)
        if not company:
            raise RuntimeError(f"Company con ID {comp_id} non trovata")

        acc_name = company.get_key_value("Account_Name") or "Company"

        # 3. Crea cartella della Company
        comp_path = f"/Accounts/{_safe(acc_name)}"
        _ensure_folder(comp_path)

        # 4. Crea cartella "03. Rounds"
        mon_root = f"{comp_path}/{_safe(parent_folder_name)}"
        _ensure_folder(mon_root)

        # 5. Sottocartella datata "YYYY MM DD Round"
        dated_folder = f"{mon_root}/{date.today():%Y %m %d} Round"
        folder_id = _ensure_folder(dated_folder)
        link = _link(folder_id)

        # 6. Salva il link nel campo SID se vuoto
        if not company.get_key_value(self.company_sid_field):
            set_field(self.parent_module, comp_id, self.company_sid_field, link)

        return link

