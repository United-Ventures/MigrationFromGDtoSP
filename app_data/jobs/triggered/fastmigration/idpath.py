#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_migration_full.py
----------------------
Converte migration_report.csv (generato dal migratore Drive ➜ SharePoint)
in:
    • migration_full.csv (con link completi)
    • migration_paths_only.csv (con path leggibili)

Entrambi con colonne:
    Google ID, SharePoint Path

• Tiene solo le righe cartella (is_folder == "true")
• Aggiunge sempre il prefisso Deals/
• Costruisce il link AllItems.aspx con parametro viewid fisso
"""

from pathlib import Path
import pandas as pd
import urllib.parse
import sys

# ---------- CONFIG -------------------------------------------------------------
DOMAIN      = "https://unitedventuressgr.sharepoint.com"
SITE_NAME   = "Etabeta-ERP"
LIBRARY     = "Shared Documents"
DEALS_DIR   = "Deals"
VIEW_ID     = "07e6e92a-e871-4d62-9513-bf8f13cf1f00"

SRC_CSV     = Path("migration_report.csv")
DST_LINKS   = Path("migration_full.csv")
DST_PATHS   = Path("migration_paths_only.csv")
# ------------------------------------------------------------------------------


def build_sp_link(rel_folder: str) -> str:
    """Ritorna il link AllItems.aspx con id=... & viewid=..."""
    path_in_site = f"/sites/{SITE_NAME}/{LIBRARY}/{DEALS_DIR}/{rel_folder}"
    id_param = urllib.parse.quote(path_in_site, safe="")
    base = f"{DOMAIN}/sites/{SITE_NAME}/{LIBRARY.replace(' ', '%20')}"
    return f"{base}/Forms/AllItems.aspx?id={id_param}&viewid={VIEW_ID}"

def build_clean_path(rel_folder: str) -> str:
    """Ritorna solo il path leggibile, senza link"""
    return f"/sites/{SITE_NAME}/{LIBRARY}/{DEALS_DIR}/{rel_folder}"


def main() -> None:
    if not SRC_CSV.exists():
        sys.exit(f"❌ File non trovato: {SRC_CSV.resolve()}")

    df = pd.read_csv(SRC_CSV, dtype=str)

    folders = df[df["is_folder"].str.lower() == "true"].copy()
    if folders.empty:
        sys.exit("⚠️  Nessuna riga cartella trovata nel report!")

    # Scegli l'ID: se esiste e non è vuoto 'gdriveextension__Drive_Folder_ID', usalo; altrimenti usa 'g_id'
    if "gdriveextension__Drive_Folder_ID" in folders.columns:
        folders["Google ID"] = folders["gdriveextension__Drive_Folder_ID"].fillna("").where(
            folders["gdriveextension__Drive_Folder_ID"].str.strip() != "", folders["g_id"]
        )
    else:
        folders["Google ID"] = folders["g_id"]

    folders["SharePoint Link"] = folders["sp_path"].apply(build_sp_link)
    folders["SharePoint Path"] = folders["sp_path"].apply(build_clean_path)

    # Salva con link
    folders[["Google ID", "SharePoint Link"]].to_csv(
        DST_LINKS, index=False, encoding="utf-8"
    )
    print(f"✅  Creato {DST_LINKS.resolve()} — {len(folders)} righe")

    # Salva con path pulito
    folders[["Google ID", "SharePoint Path"]].to_csv(
        DST_PATHS, index=False, encoding="utf-8"
    )
    print(f"✅  Creato {DST_PATHS.resolve()} — {len(folders)} righe")


if __name__ == "__main__":
    main()
