#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit une liste de SIRET avec l'API Sirene (INSEE).

Prérequis :
  - Python 3.9+
  - pip install requests pandas openpyxl
  - Identifiants INSEE (client_id / client_secret) fournis soit :
      * via variables d'environnement INSEE_CLIENT_ID et INSEE_CLIENT_SECRET
      * ou via arguments --client-id / --client-secret
      * ou via un fichier .env dans le même dossier (lignes: INSEE_CLIENT_ID=..., INSEE_CLIENT_SECRET=...)

Entrées :
  - CSV des SIRET: /mnt/data/sirets_nettoyes.csv (colonnes: siret, siren, nic) ou passez un chemin via --input
Sortie :
  - Excel enrichi: /mnt/data/enrichi_insee.xlsx (feuille "enrichi")

Usage minimal :
  python enrich_sirets_insee.py --client-id XXXX --client-secret YYYY

Usage complet :
  python enrich_sirets_insee.py --input ./sirets_nettoyes.csv --output ./enrichi_insee.xlsx --client-id XXXX --client-secret YYYY
"""
import os
import sys
import time
import json
import argparse
from pathlib import Path

import requests
import pandas as pd

# --- ⚠️ ATTENTION: identifiants codés en dur (peu sécurisé) ---
DEFAULT_CLIENT_ID = "eleffa@gmail.com"
DEFAULT_CLIENT_SECRET = "Lubelm@2021!Q"
# -------------------------------------------------------------

TOKEN_URL = os.getenv("INSEE_TOKEN_URL", "https://api.insee.fr/token")
SIRET_URL_TMPL = os.getenv("INSEE_SIRET_URL_TMPL", "https://api.insee.fr/entreprises/sirene/V3/siret/{siret}")

def read_env_file(env_path: Path) -> dict:
    d = {}
    try:
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                d[k.strip()] = v.strip().strip('"').strip("'")
    except Exception:
        pass
    return d

def get_token(client_id: str, client_secret: str) -> str:
    r = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=20,
    )
    r.raise_for_status()
    return r.json().get("access_token")

def extract_fields(payload: dict) -> dict:
    etab = payload.get("etablissement") or payload
    unite = etab.get("uniteLegale", {})
    periodes_etab = etab.get("periodesEtablissement") or []
    periode = periodes_etab[0] if periodes_etab else {}

    adr_fields = [
        "numeroVoieEtablissement", "indiceRepetitionEtablissement", "typeVoieEtablissement",
        "libelleVoieEtablissement", "complementAdresseEtablissement"
    ]
    adresse_l1 = " ".join(str(periode.get(k) or etab.get(k) or "").strip() for k in adr_fields if (periode.get(k) or etab.get(k)))
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

def resolve_credentials(cli_id: str, cli_secret: str) -> tuple[str, str]:
    # Priority: CLI args > ENV vars > .env file in script directory
    if cli_id and cli_secret:
        return cli_id, cli_secret

    env_id = os.getenv("INSEE_CLIENT_ID")
    env_secret = os.getenv("INSEE_CLIENT_SECRET")
    if env_id and env_secret:
        return env_id, env_secret

    env_file = Path(__file__).resolve().parent / ".env"
    parsed = read_env_file(env_file)
    file_id = parsed.get("INSEE_CLIENT_ID")
    file_secret = parsed.get("INSEE_CLIENT_SECRET")
    if file_id and file_secret:
        return file_id, file_secret

    return None, None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="/mnt/data/sirets_nettoyes.csv", help="CSV avec une colonne 'siret'")
    parser.add_argument("--output", type=str, default="/mnt/data/enrichi_insee.xlsx")
    parser.add_argument("--sleep", type=float, default=0.2, help="Pause entre requêtes (s)")
    parser.add_argument("--client-id", type=str, default=None, help="INSEE client_id (remplace la variable d'environnement)")
    parser.add_argument("--client-secret", type=str, default=None, help="INSEE client_secret (remplace la variable d'environnement)")
    args = parser.parse_args()

    client_id, client_secret = resolve_credentials(args.client_id, args.client_secret)
    if not client_id or not client_secret:
        print("ERREUR: fournissez vos identifiants INSEE.\n"
              "- Variables d'env: INSEE_CLIENT_ID et INSEE_CLIENT_SECRET\n"
              "- OU arguments: --client-id ... --client-secret ...\n"
              "- OU fichier .env à côté du script avec:\n"
              "    INSEE_CLIENT_ID=xxx\n"
              "    INSEE_CLIENT_SECRET=yyy", file=sys.stderr)
        sys.exit(1)

    try:
        token = get_token(client_id, client_secret)
    except requests.HTTPError as e:
        msg = f"Echec d'authentification ({e.response.status_code}). Vérifiez vos identifiants et droits d'accès."
        print(msg, file=sys.stderr)
        sys.exit(2)

    headers = {"Authorization": f"Bearer {token}"}

    df = pd.read_csv(args.input, dtype=str)
    df["siret"] = df["siret"].astype(str).str.replace(r"\D+", "", regex=True)

    rows = []
    for siret in df["siret"].dropna().unique():
        if len(siret) != 14:
            rows.append({"siret": siret, "erreur": "SIRET invalide"})
            continue
        url = SIRET_URL_TMPL.format(siret=siret)
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 401:
                # Token expiré : régénérer une fois
                token = get_token(client_id, client_secret)
                headers["Authorization"] = f"Bearer {token}"
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
