import pandas as pd
import time
import traceback
from ZConnector import *
from datetime import datetime
from typing import List
from zcrmsdk.src.com.zoho.crm.api.record import Record as ZCRMRecord

# ------------------------------------------------------------------------------

CSV_PATH       = "migration_paths_only.csv"  # deve contenere la colonna “Google ID”
SID_FIELD      = "sid"
SLEEP_SEC      = 0.2

SDKInitializer.initialize()

def log(msg: str):
    print(f"{datetime.now().isoformat()} | {msg}")

def PerformQuery(queryInstructions):
    from zcrmsdk.src.com.zoho.crm.api.query import QueryOperations
    from zcrmsdk.src.com.zoho.crm.api.query import BodyWrapper
    from zcrmsdk.src.com.zoho.crm.api.query import ResponseWrapper, APIException

    returnRecords = []
    query_operations = QueryOperations()
    body_wrapper = BodyWrapper()
    body_wrapper.set_select_query(queryInstructions)
    response = query_operations.get_records(body_wrapper)

    if response is not None:
        print('Status Code:', response.get_status_code())
        if response.get_status_code() in [204, 304]:
            return []
        response_object = response.get_object()
        if isinstance(response_object, ResponseWrapper):
            return response_object.get_data()
        elif isinstance(response_object, APIException):
            print("❌ COQL ERROR:", response_object.get_message().get_value())
            return []

def SetRecordField(module, id, fName, value):
    from zcrmsdk.src.com.zoho.crm.api.record import (
        RecordOperations, BodyWrapper, ActionWrapper, APIException, SuccessResponse
    )
    record_operations = RecordOperations()
    request = BodyWrapper()
    record = ZCRMRecord()
    record.add_key_value(fName, str(value))
    request.set_data([record])
    response = record_operations.update_record(id, module, request)
    print("Status Code:", response.get_status_code())
    response_object = response.get_object()

    if isinstance(response_object, ActionWrapper):
        for action_response in response_object.get_data():
            if isinstance(action_response, SuccessResponse):
                print("✔️ Successo:", action_response.get_message().get_value())
            elif isinstance(action_response, APIException):
                print("❌ Errore API:", action_response.get_message().get_value())
    elif isinstance(response_object, APIException):
        print("❌ Errore globale:", response_object.get_message().get_value())

def load_csv_mapping(csv_path: str) -> dict[str, str]:
    df = pd.read_csv(csv_path)
    df["Google ID"] = df["Google ID"].astype(str).str.strip()
    return dict(zip(df["Google ID"], df["SharePoint Path"]))

def _update_sid_by_field(module: str, gid_field: str, gid: str, new_path: str, dry_run=False) -> bool:
    query = f'SELECT id, {SID_FIELD} FROM {module} WHERE {gid_field} = "{gid}"'
    recs = PerformQuery(query)
    if not recs:
        log(f"❌  {module}: nessun record con {gid_field} = {gid}")
        return False

    rec = recs[0]
    rec_id = rec.get_id()
    old_sid = rec.get_key_value(SID_FIELD)
    log(f"✅  {module[:-1]} {rec_id} – {gid_field} matchato – SID attuale: {old_sid!s} → nuovo: {new_path}")

    if dry_run:
        log("    (dry-run, skip UPDATE)")
        return True

    try:
        SetRecordField(module, rec_id, SID_FIELD, new_path)
        log("    → aggiornato ✔︎")
        return True
    except Exception:
        log(f"⚠️  UPDATE fallito:\n{traceback.format_exc()}")
        return False

# ----------------------------------------------------------------------
# MAIN LOGIC
# ----------------------------------------------------------------------

def update_all_sids(csv_path: str, dry_run: bool = True):
    mapping = load_csv_mapping(csv_path)
    log(f"🚀 CSV caricato – {len(mapping)} righe – dry_run={dry_run}")

    for gid, path in mapping.items():
        # 1. Deals → cerca gdriveextension__Drive_Folder_ID
        if _update_sid_by_field("Deals", "gdriveextension__Drive_Folder_ID", gid, path, dry_run=dry_run):
            time.sleep(SLEEP_SEC)
            continue

        # 2. Deals → fallback su gid
        if _update_sid_by_field("Deals", "gid", gid, path, dry_run=dry_run):
            time.sleep(SLEEP_SEC)
            continue

        # 3. Accounts → fallback finale su gdriveextension__Drive_Folder_ID
        if _update_sid_by_field("Accounts", "gdriveextension__Drive_Folder_ID", gid, path, dry_run=dry_run):
            time.sleep(SLEEP_SEC)
            continue

        # 4. Nulla trovato
        log(f"❌ GID {gid} – nessun match trovato in Deals né Accounts")
        time.sleep(SLEEP_SEC)

    log("🎯  FINITO")

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    update_all_sids(CSV_PATH, dry_run=False)
