#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_migration_full.py
----------------------
Converte migration_report.csv (generato dal migratore Drive ➜ SharePoint)
in migration_full.csv, con colonne:
    Google ID, SharePoint Path

• Tiene solo le righe cartella (is_folder == "true")
• Aggiunge sempre il prefisso  Deals/
• Costruisce il link completo AllItems.aspx con parametro viewid fisso
• Non deduplica: ogni Google ID rimane

Esegui semplicemente:  python make_migration_full.py
"""

from pathlib import Path
import pandas as pd
import urllib.parse
import sys

# ---------- CONFIG -------------------------------------------------------------
DOMAIN      = "https://unitedventuressgr.sharepoint.com"
SITE_NAME   = "Etabeta-ERP"
LIBRARY     = "Shared Documents"
DEALS_DIR   = "Deals"                     # sottocartella fissa
VIEW_ID     = "07e6e92a-e871-4d62-9513-bf8f13cf1f00"

SRC_CSV     = Path("migration_report.csv")   # input
DST_CSV     = Path("migration_full.csv")     # output
# ------------------------------------------------------------------------------


def build_sp_link(rel_folder: str) -> str:
    """
    Ritorna il link AllItems.aspx con id=... & viewid=...
    dove rel_folder è il nome cartella (già con eventuale prefisso ID_)
    """
    # /sites/<Site>/Shared Documents/Deals/<Folder>
    path_in_site = f"/sites/{SITE_NAME}/{LIBRARY}/{DEALS_DIR}/{rel_folder}".replace(" ", " ")
    # encode TUTTO (anche gli /) → %2F...
    id_param = urllib.parse.quote(path_in_site, safe="")
    # parte iniziale del link (Shared%20Documents già codificato per la UI)
    base = f"{DOMAIN}/sites/{SITE_NAME}/{LIBRARY.replace(' ', '%20')}"
    return f"{base}/Forms/AllItems.aspx?id={id_param}&viewid={VIEW_ID}"


def main() -> None:
    if not SRC_CSV.exists():
        sys.exit(f"❌ File non trovato: {SRC_CSV.resolve()}")

    df = pd.read_csv(SRC_CSV, dtype=str)

    folders = df[df["is_folder"].str.lower() == "true"].copy()
    if folders.empty:
        sys.exit("⚠️  Nessuna riga cartella trovata nel report!")

    # Costruisci percorso SharePoint
    folders["SharePoint Path"] = folders["sp_path"].apply(build_sp_link)

    # Rinominare la colonna g_id → Google ID
    folders.rename(columns={"g_id": "Google ID"}, inplace=True)

    # Salva file finale
    folders[["Google ID", "SharePoint Path"]].to_csv(
        DST_CSV, index=False, encoding="utf-8"
    )
    print(f"✅  Creato {DST_CSV.resolve()} — {len(folders)} righe")


if __name__ == "__main__":
    main()
