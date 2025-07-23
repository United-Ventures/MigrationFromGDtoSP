import pandas as pd
import time
import traceback
from ZConnector import *
from datetime import datetime
from typing import List, Dict, Tuple
from zcrmsdk.src.com.zoho.crm.api.record import Record as ZCRMRecord
# ------------------------------------------------------------------------------

CSV_PATH       = "migration_full.csv"  # deve contenere la colonna “Google ID”
MODULE         = "Deals"
GID_FIELD      = "gid"                       # campo Zoho con il Google-ID
SID_FIELD      = "sid"                       # API-name del tuo custom field SID
SLEEP_SEC      = 0.2                         # throttle per sicurezza

# altri campi che vuoi visualizzare (API-name → etichetta di colonna)
FIELDS_TO_READ = {
    SID_FIELD          : "sid attuale",
    "Stage"            : "Stage",
    "Account_Name"     : "Account",          # lookup: Zoho restituisce oggetto
}
# -- UTILITIES ------------------------------------------------------------ #

SDKInitializer.initialize()

def PerformQuery(queryInstructions):
    from zcrmsdk.src.com.zoho.crm.api.query import QueryOperations
    """
    This method is used to get records from the module through a COQL query.
    """
    from zcrmsdk.src.com.zoho.crm.api.query import BodyWrapper
    from zcrmsdk.src.com.zoho.crm.api.query import ResponseWrapper, APIException
    returnRecords = []
    # Get instance of QueryOperations Class
    query_operations = QueryOperations()
    # Get instance of BodyWrapper Class that will contain the request body
    body_wrapper = BodyWrapper()
    select_query = queryInstructions

    # Set the select_query to BodyWrapper instance
    body_wrapper.set_select_query(select_query)
    # Call get_records method that takes BodyWrapper instance as parameter
    response = query_operations.get_records(body_wrapper)

    if response is not None:
        # Get the status code from response
        print('Status Code: ' + str(response.get_status_code()))
        if response.get_status_code() in [204, 304]:
            print('No Content' if response.get_status_code() == 204 else 'Not Modified')
            return

        # Get object from response
        response_object = response.get_object()
        if response_object is not None:
            # Check if expected ResponseWrapper instance is received.
            if isinstance(response_object, ResponseWrapper):
                # Get the list of obtained Record instances
                record_list = response_object.get_data()
                for record in record_list:
                    returnRecords.append(record)
                return returnRecords
            # Check if the request returned an exception
            elif isinstance(response_object, APIException):
                # Get the Status
                print("Status: " + response_object.get_status().get_value())
                # Get the Code
                print("Code: " + response_object.get_code().get_value())
                print("Details")
                # Get the details dict
                details = response_object.get_details()
                for key, value in details.items():
                    print(key + ' : ' + str(value))
                # Get the Message
                print("Message: " + response_object.get_message().get_value())

def SetRecordField(module, id, fName, value):
    from zcrmsdk.src.com.zoho.crm.api.record import (
        RecordOperations,
        BodyWrapper,
        ActionWrapper,
        APIException,
        SuccessResponse
    )
    from zcrmsdk.src.com.zoho.crm.api.record import Record as ZCRMRecord

    record_operations = RecordOperations()
    request = BodyWrapper()
    records_list = []
    record = ZCRMRecord()
    record.add_key_value(fName, str(value))  # Forza stringa
    records_list.append(record)
    request.set_data(records_list)

    response = record_operations.update_record(id, module, request)
    print("Status Code:", response.get_status_code())

    response_object = response.get_object()

    if isinstance(response_object, ActionWrapper):
        for action_response in response_object.get_data():
            if isinstance(action_response, SuccessResponse):
                print("✔️ Successo:", action_response.get_message().get_value())
            elif isinstance(action_response, APIException):
                print("❌ Errore API:")
                print("Status:", action_response.get_status().get_value())
                print("Code:", action_response.get_code().get_value())
                print("Message:", action_response.get_message().get_value())
                print("Details:", action_response.get_details())
    elif isinstance(response_object, APIException):
        print("❌ Errore globale:")
        print("Status:", response_object.get_status().get_value())
        print("Code:", response_object.get_code().get_value())
        print("Message:", response_object.get_message().get_value())
        print("Details:", response_object.get_details())

#  ─── la funzione richiesta ────────────────────────────────────────────────────
def log(msg: str):
    print(f"{datetime.now().isoformat()} | {msg}")

def load_csv_mapping(csv_path: str) -> dict[str, str]:
    """Legge il CSV solo una volta e restituisce {GID: SharePointPath}."""
    df = pd.read_csv(csv_path)
    return dict(zip(df["Google ID"].astype(str), df["SharePoint Path"]))

# ------------------------------------------------------------------ #
# CORE                                                               #
# ------------------------------------------------------------------ #
def _update_sid_generic(module: str,               # "Deals" o "Accounts"
                        gid_field: str,            # es. "gid" o "gdriveextension__Drive_Folder_ID"
                        sid_field: str,            # di solito "sid"
                        gid: str,
                        new_path: str,
                        dry_run: bool = False):
    """
    Una singola operazione di update:
      1. COQL SELECT id,sid_field WHERE gid_field = gid
      2. se trovato → SetRecordField(module,id,sid_field,new_path)
    """
    query = f'SELECT id, {sid_field} FROM {module} WHERE {gid_field} = "{gid}"'
    try:
        recs = PerformQuery(query) or []
    except Exception as e:
        log(f"⚠️  COQL error ({module}, GID={gid}): {e}")
        return

    if not recs:
        log(f"❌  {module}: nessun record con GID {gid}")
        return

    rec = recs[0]
    rec_id   = rec.get_id()
    old_sid  = rec.get_key_value(sid_field)
    log(f"✅  {module[:-1]} {rec_id}  –  SID attuale: {old_sid!s}  →  nuovo: {new_path}")

    if dry_run:
        log("    (dry-run, skip UPDATE)")
        return

    try:
        SetRecordField(module, rec_id, sid_field, new_path)
        log("    → aggiornato ✔︎")
    except Exception:
        log(f"⚠️  UPDATE fallito:\n{traceback.format_exc()}")

# ------------------------------------------------------------------ #
# WRAPPERS specifici                                                 #
# ------------------------------------------------------------------ #
def update_deal_sid(gid: str, new_path: str, *, dry_run=False):
    _update_sid_generic(
        module="Deals",
        gid_field="gid",
        sid_field="sid",
        gid=gid,
        new_path=new_path,
        dry_run=dry_run
    )

def update_account_sid(gid: str, new_path: str, *, dry_run=False):
    _update_sid_generic(
        module="Accounts",
        gid_field="gdriveextension__Drive_Folder_ID",
        sid_field="sid",
        gid=gid,
        new_path=new_path,
        dry_run=dry_run
    )

# ------------------------------------------------------------------ #
# ORCHESTRATORE                                                      #
# ------------------------------------------------------------------ #
def update_all_sids(csv_path: str, dry_run: bool = True):
    mapping = load_csv_mapping(csv_path)
    log(f"🚀 CSV caricato – {len(mapping)} righe – dry_run={dry_run}")

    # Pass 1: DEALS
    log("▶️  Aggiorno DEALS")
    for gid, path in mapping.items():
        update_deal_sid(gid, path, dry_run=dry_run)
        time.sleep(0.2)          # throttling leggero

    # Pass 2: ACCOUNTS
    log("▶️  Aggiorno ACCOUNTS")
    for gid, path in mapping.items():
        update_account_sid(gid, path, dry_run=dry_run)
        time.sleep(0.2)

    log("🎯  FINITO")
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    CSV_PATH = "migration_cleaned_paths.csv"
    update_all_sids(CSV_PATH, dry_run=False)