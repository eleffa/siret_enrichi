#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit une liste de SIRET avec l'API Sirene (INSEE).
Prérequis :
  - Python 3.9+
  - pip install requests pandas openpyxl
  - Variables d'environnement INSEE_CLIENT_ID et INSEE_CLIENT_SECRET
Entrées :
  - /mnt/data/sirets_nettoyes.csv (colonnes: siret, siren, nic) ou passez un chemin via --input
Sortie :
  - /mnt/data/enrichi_insee.xlsx (feuille "enrichi")
Usage :
  python enrich_sirets_insee.py --input /path/to/sirets.csv --output /path/to/enrichi.xlsx
"""
import os
import sys
import time
import json
import argparse
from pathlib import Path

import requests
import pandas as pd

TOKEN_URL = os.getenv("INSEE_TOKEN_URL", "https://api.insee.fr/token")
# Selon la doc V3, l'API Sirene expose les endpoints /entreprises/sirene/V3/siret/{siret}
SIRET_URL_TMPL = os.getenv("INSEE_SIRET_URL_TMPL", "https://api.insee.fr/entreprises/sirene/V3/siret/{siret}")

def get_token(client_id: str, client_secret: str) -> str:
    r = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def extract_fields(payload: dict) -> dict:
    """
    Extrait un sous-ensemble de champs utiles depuis la réponse API (structure V3).
    La structure exacte peut évoluer ; on fait donc des .get() défensifs.
    """
    etab = payload.get("etablissement") or payload
    unite = etab.get("uniteLegale", {})
    periodes_etab = etab.get("periodesEtablissement") or etab.get("periodesEtablissementUniteLegale") or []
    periode = periodes_etab[0] if periodes_etab else {}

    # Adresse
    adr_fields = [
        "numeroVoieEtablissement", "indiceRepetitionEtablissement", "typeVoieEtablissement",
        "libelleVoieEtablissement", "complementAdresseEtablissement"
    ]
    adresse_l1 = " ".join(str(periode.get(k, "") or etab.get(k, "")).strip() for k in adr_fields if (periode.get(k) or etab.get(k)))
    code_postal = periode.get("codePostalEtablissement") or etab.get("codePostalEtablissement")
    commune = periode.get("libelleCommuneEtablissement") or etab.get("libelleCommuneEtablissement")

    naf = periode.get("activitePrincipaleEtablissement") or etab.get("activitePrincipaleEtablissement")
    lib_naf = periode.get("nomenclatureActivitePrincipaleEtablissement") or ""

    return {
        "siret": etab.get("siret"),
        "siren": unite.get("siren") or (etab.get("siret") or "")[:9],
        "denomination": unite.get("denominationUniteLegale") or unite.get("nomUniteLegale"),
        "enseigne": periode.get("enseigne1Etablissement") or etab.get("enseigne1Etablissement"),
        "nom_commercial": periode.get("nomCommercialEtablissement") or etab.get("nomCommercialEtablissement"),
        "statut_diffusion": unite.get("statutDiffusionUniteLegale"),
        "etat_etablissement": periode.get("etatAdministratifEtablissement") or etab.get("etatAdministratifEtablissement"),
        "date_creation": unite.get("dateCreationUniteLegale"),
        "tranche_effectifs": unite.get("trancheEffectifsUniteLegale"),
        "naf": naf,
        "libelle_naf": lib_naf,
        "adresse_ligne_1": adresse_l1 or None,
        "code_postal": code_postal,
        "commune": commune,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="/mnt/data/sirets_nettoyes.csv")
    parser.add_argument("--output", type=str, default="/mnt/data/enrichi_insee.xlsx")
    parser.add_argument("--sleep", type=float, default=0.2, help="Pause entre requêtes (s)")
    args = parser.parse_args()

    client_id = os.getenv("INSEE_CLIENT_ID")
    client_secret = os.getenv("INSEE_CLIENT_SECRET")
    if not client_id or not client_secret:
        print("ERREUR: définissez INSEE_CLIENT_ID et INSEE_CLIENT_SECRET dans vos variables d'environnement.", file=sys.stderr)
        sys.exit(1)

    token = get_token(client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}"}

    df = pd.read_csv(args.input, dtype=str)
    df["siret"] = df["siret"].astype(str).str.replace(r"\D+", "", regex=True)

    rows = []
    for siret in df["siret"].dropna().unique():
        if len(siret) != 14:
            continue
        url = SIRET_URL_TMPL.format(siret=siret)
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 401:
                # Token expiré : on le régénère une fois
                token = get_token(client_id, client_secret)
                headers = {"Authorization": f"Bearer {token}"}
                r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
            rows.append(extract_fields(data))
        except Exception as e:
            rows.append({"siret": siret, "erreur": str(e)})

        time.sleep(args.sleep)

    out = pd.DataFrame(rows)
    out.to_excel(args.output, index=False)
    print(f"OK -> {args.output}")

if __name__ == "__main__":
    main()
