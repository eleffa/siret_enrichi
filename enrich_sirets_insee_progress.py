#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
enrich_sirets_insee_progress.py
--------------------------------
Enrichit une liste de SIRET via l'API Sirene (INSEE) **avec affichage d'avancement**.

✅ Deux modes pris en charge :
  1) API key (nouveau portail, recommandé) -> header: X-INSEE-Api-Key-Integration, base: https://api.insee.fr/api-sirene/{ver}
  2) OAuth2 Client Credentials (plans spécifiques) -> token + base: https://api.insee.fr/entreprises/sirene/V3

🧰 Dépendances :
  pip install requests pandas openpyxl

🖥️ Exemples :
  # Mode API-key, avec une barre de progression + logs toutes les 10 lignes
  python enrich_sirets_insee_progress.py \
    --input ./sirets_nettoyes.csv \
    --output ./enrichi_insee.xlsx \
    --api-key VOTRE_CLE \
    --api-version 3.11 \
    --every 10 --verbose

  # Mode OAuth2
  python enrich_sirets_insee_progress.py \
    --input ./sirets_nettoyes.csv \
    --output ./enrichi_insee.xlsx \
    --client-id XXX --client-secret YYY \
    --oauth-base https://api.insee.fr/entreprises/sirene/V3 \
    --every 10 --verbose
"""
import os
import sys
import time
import csv
import argparse
from pathlib import Path

import requests
import pandas as pd

# --------- Constantes ---------
DEFAULT_API_VERSION = "3.11"
APIKEY_BASE_TMPL = "https://api.insee.fr/api-sirene/{ver}"
OAUTH_BASE_DEFAULT = "https://api.insee.fr/entreprises/sirene/V3"
TOKEN_URL = "https://api.insee.fr/token"

# --------- TQDM (optionnel) ---------
def get_tqdm(total):
    try:
        from tqdm import tqdm  # type: ignore
        return tqdm(total=total, unit="siret", dynamic_ncols=True)
    except Exception:
        # Fallback minimaliste
        class Dummy:
            def __init__(self, total): self.total, self.n = total, 0
            def update(self, v=1): self.n += v
            def close(self): pass
        return Dummy(total)

# --------- Utilitaires ---------
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
    # Réponse 3.11: l'objet principal contient "etablissement"
    etab = payload.get("etablissement") or payload
    unite = etab.get("uniteLegale", {})

    # Adresse
    adr = etab.get("adresseEtablissement", {}) or {}
    numero = adr.get("numeroVoieEtablissement") or ""
    type_voie = adr.get("typeVoieEtablissement") or ""
    lib_voie = adr.get("libelleVoieEtablissement") or ""
    comp = adr.get("complementAdresseEtablissement") or ""
    adresse_l1 = " ".join(x for x in [str(numero), type_voie, lib_voie, comp] if x).strip() or None
    code_postal = adr.get("codePostalEtablissement")
    commune = adr.get("libelleCommuneEtablissement")

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

def write_checkpoint_csv(rows, path_csv, fieldnames):
    # Écrit/écrase un CSV (checkpoint) pour visualiser l'avancement pendant l'exécution
    with open(path_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="./sirets_nettoyes.csv", help="CSV avec une colonne 'siret'")
    parser.add_argument("--output", type=str, default="./enrichi_insee.xlsx")
    parser.add_argument("--sleep", type=float, default=0.1, help="Pause entre requêtes (s)")
    parser.add_argument("--timeout", type=float, default=30.0, help="Timeout requêtes (s)")
    parser.add_argument("--max-retries", type=int, default=2, help="Nombre de retries par SIRET en cas d'échec réseau/429")
    parser.add_argument("--retry-backoff", type=float, default=0.8, help="Backoff (s) entre retries")
    parser.add_argument("--every", type=int, default=10, help="Afficher une ligne de log toutes les N requêtes")
    parser.add_argument("--verbose", action="store_true", help="Affiche chaque SIRET traité")
    parser.add_argument("--log-file", type=str, default=None, help="Fichier log texte (append)")
    parser.add_argument("--checkpoint-rows", type=int, default=50, help="Écrire un CSV intermédiaire toutes les N lignes")
    # API-key
    parser.add_argument("--api-key", type=str, default=None, help="Clé d'API (X-INSEE-Api-Key-Integration). Peut aussi venir de INSEE_API_KEY ou .env")
    parser.add_argument("--api-version", type=str, default=DEFAULT_API_VERSION, help="Version d'API Sirene (ex: 3.11)")
    # OAuth2
    parser.add_argument("--client-id", type=str, default=None, help="INSEE client_id (OAuth2)")
    parser.add_argument("--client-secret", type=str, default=None, help="INSEE client_secret (OAuth2)")
    parser.add_argument("--oauth-base", type=str, default=OAUTH_BASE_DEFAULT, help="Base URL pour OAuth2")

    args = parser.parse_args()

    # Logs -> fichier (optionnel)
    def log(msg):
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()
        if args.log_file:
            with open(args.log_file, "a", encoding="utf-8") as lf:
                lf.write(msg + "\n")

    # Charger SIRETs
    df = pd.read_csv(args.input, dtype=str)
    df["siret"] = df["siret"].astype(str).str.replace(r"\D+", "", regex=True)
    sirets = [s for s in df["siret"].dropna().unique() if len(s) == 14]
    total = len(sirets)
    if total == 0:
        log("Aucun SIRET valide (14 chiffres) trouvé.")
        sys.exit(0)

    # Résolution API-key / OAuth2
    env = read_env_file(Path(".") / ".env")
    api_key = args.api_key or os.getenv("INSEE_API_KEY") or env.get("INSEE_API_KEY")

    session = requests.Session()
    if api_key:
        base_url = APIKEY_BASE_TMPL.format(ver=args.api_version)
        headers = {"X-INSEE-Api-Key-Integration": api_key, "Accept": "application/json"}
        def build_url(s): return f"{base_url}/siret/{s}"
        mode = f"API-KEY (v{args.api_version})"
    else:
        client_id = args.client_id or os.getenv("INSEE_CLIENT_ID") or env.get("INSEE_CLIENT_ID")
        client_secret = args.client_secret or os.getenv("INSEE_CLIENT_SECRET") or env.get("INSEE_CLIENT_SECRET")
        if not client_id or not client_secret:
            log("ERREUR: fournissez --api-key (recommandé) OU --client-id/--client-secret (OAuth2).")
            sys.exit(1)
        token = get_token(client_id, client_secret)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        def build_url(s): return f"{args.oauth_base.rstrip('/')}/siret/{s}"
        mode = "OAUTH2"

    log(f"Mode: {mode} | Total SIRET à traiter: {total}")

    rows = []
    ok, ko = 0, 0
    checkpoint_csv = Path(args.output).with_suffix(".checkpoint.csv")

    pbar = get_tqdm(total)
    for i, s in enumerate(sirets, start=1):
        url = build_url(s)

        # retries simples (réseau/429)
        attempt = 0
        last_err = None
        while attempt <= args.max_retries:
            try:
                r = session.get(url, headers=headers, timeout=args.timeout)
                if r.status_code == 429:
                    # Respecter Retry-After si présent
                    ra = r.headers.get("Retry-After")
                    pause = float(ra) if ra and ra.isdigit() else args.retry_backoff
                    time.sleep(pause)
                    attempt += 1
                    continue
                r.raise_for_status()
                js = r.json()
                rows.append(extract_fields(js))
                ok += 1
                last_err = None
                break
            except Exception as e:
                last_err = e
                attempt += 1
                time.sleep(args.retry_backoff)

        if last_err is not None:
            rows.append({"siret": s, "erreur": str(last_err)})
            ko += 1

        if args.verbose:
            status = "OK" if last_err is None else f"ERREUR: {last_err}"
            log(f"[{i}/{total}] {s} -> {status}")

        if i % args.every == 0:
            log(f"Progression: {i}/{total} traités | OK={ok} KO={ko}")

        if args.checkpoint_rows > 0 and (i % args.checkpoint_rows == 0 or i == total):
            # Sauvegarde checkpoint CSV visible en cours de route
            fieldnames = sorted({k for row in rows for k in row.keys()})
            write_checkpoint_csv(rows, checkpoint_csv, fieldnames)
            # Indique le chemin pour consultation
            log(f"Checkpoint écrit: {checkpoint_csv} (lignes={len(rows)})")

        pbar.update(1)
        time.sleep(args.sleep)

    pbar.close()

    # Export final Excel
    out = pd.DataFrame(rows)
    out.to_excel(args.output, index=False)

    log("")
    log("----------- RÉSUMÉ -----------")
    log(f"Total: {total} | OK: {ok} | KO: {ko}")
    log(f"Fichier Excel: {args.output}")
    log(f"CSV checkpoint (dernier): {checkpoint_csv}")
    log("------------------------------")

if __name__ == "__main__":
    main()
