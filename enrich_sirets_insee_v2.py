#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit une liste de SIRET avec l'API Sirene (INSEE).

Deux modes pris en charge :
  1) **API key (recommandé)** via header `X-INSEE-Api-Key-Integration` et base URL `https://api.insee.fr/api-sirene/{version}`
  2) **OAuth2 Client Credentials** (plans spécifiques) via `POST https://api.insee.fr/token` puis base URL `https://api.insee.fr/entreprises/sirene/V3`

Prérequis :
  - Python 3.9+
  - pip install requests pandas openpyxl
  - Fichier CSV: une colonne "siret" (cf. sirets_nettoyes.csv)

Exemples :
  # Mode API key (nouveau portail — le plus simple)
  python enrich_sirets_insee_v2.py --input ./sirets_nettoyes.csv --output ./enrichi_insee.xlsx --api-key VOTRE_CLE --api-version 3.11

  # Mode OAuth2 (si votre plan l'exige)
  python enrich_sirets_insee_v2.py --input ./sirets_nettoyes.csv --output ./enrichi_insee.xlsx --client-id XXX --client-secret YYY --oauth-base https://api.insee.fr/entreprises/sirene/V3
"""
import os
import sys
import time
import argparse
from pathlib import Path

import requests
import pandas as pd

# --- Defaults ---
DEFAULT_API_VERSION = "3.11"
APIKEY_BASE_TMPL = "https://api.insee.fr/api-sirene/{ver}"  # API-key mode
OAUTH_BASE_DEFAULT = "https://api.insee.fr/entreprises/sirene/V3"  # OAuth2 mode
TOKEN_URL = "https://api.insee.fr/token"  # OAuth2 token endpoint

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
    js = r.json()
    return js.get("access_token")

def extract_fields(payload: dict) -> dict:
    # Doc 3.11: la réponse contient "etablissement": { ... } pour l'unitaire SIRET
    etab = payload.get("etablissement") or payload
    unite = etab.get("uniteLegale", {})

    # Adresses (selon 3.11)
    adresse = etab.get("adresseEtablissement", {}) or {}
    numero = adresse.get("numeroVoieEtablissement") or ""
    type_voie = adresse.get("typeVoieEtablissement") or ""
    lib_voie = adresse.get("libelleVoieEtablissement") or ""
    comp = adresse.get("complementAdresseEtablissement") or ""
    adresse_l1 = " ".join(x for x in [str(numero), type_voie, lib_voie, comp] if x).strip() or None

    code_postal = adresse.get("codePostalEtablissement")
    commune = adresse.get("libelleCommuneEtablissement")

    periodes = etab.get("periodesEtablissement") or []
    periode = periodes[0] if periodes else {}
    naf = periode.get("activitePrincipaleEtablissement") or etab.get("activitePrincipaleEtablissement")
    lib_naf = periode.get("nomenclatureActivitePrincipaleEtablissement") or ""

    return {
        "siret": etab.get("siret"),
        "siren": (etab.get("siret") or "")[:9],
        "denomination": unite.get("denominationUniteLegale") or unite.get("nomUniteLegale"),
        "etat_etablissement": periode.get("etatAdministratifEtablissement") or etab.get("etatAdministratifEtablissement"),
        "date_creation": unite.get("dateCreationUniteLegale"),
        "tranche_effectifs": unite.get("trancheEffectifsUniteLegale") or etab.get("trancheEffectifsEtablissement"),
        "naf": naf,
        "libelle_naf": lib_naf,
        "adresse_ligne_1": adresse_l1,
        "code_postal": code_postal,
        "commune": commune,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="/mnt/data/sirets_nettoyes.csv", help="CSV avec une colonne 'siret'")
    parser.add_argument("--output", type=str, default="/mnt/data/enrichi_insee.xlsx")
    parser.add_argument("--sleep", type=float, default=0.15, help="Pause entre requêtes (s)")
    # API-key
    parser.add_argument("--api-key", type=str, default=None, help="Clé d'API INSEE (plan Public). Peut aussi être définie via INSEE_API_KEY ou .env")
    parser.add_argument("--api-version", type=str, default=DEFAULT_API_VERSION, help="Version de l'API Sirene (ex: 3.11)")
    # OAuth2
    parser.add_argument("--client-id", type=str, default=None, help="INSEE client_id (mode OAuth2)")
    parser.add_argument("--client-secret", type=str, default=None, help="INSEE client_secret (mode OAuth2)")
    parser.add_argument("--oauth-base", type=str, default=OAUTH_BASE_DEFAULT, help="Base URL pour le mode OAuth2")
    args = parser.parse_args()

    # Inputs
    df = pd.read_csv(args.input, dtype=str)
    df["siret"] = df["siret"].astype(str).str.replace(r"\D+", "", regex=True)
    sirets = [s for s in df["siret"].dropna().unique() if len(s) == 14]
    if not sirets:
        print("Aucun SIRET valide (14 chiffres) trouvé dans le fichier.", file=sys.stderr)

    # Try API-key first if provided
    env_file = Path(__file__).resolve().parent / ".env"
    env = read_env_file(env_file)
    api_key = args.api_key or os.getenv("INSEE_API_KEY") or env.get("INSEE_API_KEY")

    rows = []
    if api_key:
        base_url = APIKEY_BASE_TMPL.format(ver=args.api_version)
        headers = {"X-INSEE-Api-Key-Integration": api_key, "Accept": "application/json"}
        def build_url(s): return f"{base_url}/siret/{s}"
    else:
        # Fallback OAuth2
        client_id = args.client_id or os.getenv("INSEE_CLIENT_ID") or env.get("INSEE_CLIENT_ID")
        client_secret = args.client_secret or os.getenv("INSEE_CLIENT_SECRET") or env.get("INSEE_CLIENT_SECRET")
        if not client_id or not client_secret:
            print("ERREUR: fournissez une --api-key (recommandé) OU bien --client-id/--client-secret pour OAuth2.", file=sys.stderr)
            sys.exit(1)
        try:
            token = get_token(client_id, client_secret)
        except requests.HTTPError as e:
            code_status = getattr(e.response, "status_code", "unknown")
            print(f"Echec d'authentification ({code_status}). Vérifiez vos identifiants et droits d'accès.", file=sys.stderr)
            sys.exit(2)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        def build_url(s): return f"{args.oauth_base.rstrip('/')}/siret/{s}"

    for s in sirets:
        url = build_url(s)
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            js = r.json()
            rows.append(extract_fields(js))
        except Exception as e:
            rows.append({"siret": s, "erreur": str(e)})
        time.sleep(args.sleep)

    out = pd.DataFrame(rows)
    out.to_excel(args.output, index=False)
    print(f"OK -> {args.output}")

if __name__ == "__main__":
    main()
