#!/usr/bin/env python3
"""
Dashboard — Modulation nucléaire par réacteur (France)
Normalisation par la puissance nominale IAEA PRIS

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CACHE CLOUD (Parquet + JSON sur Cloudflare R2 / S3)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Les données sont mises en cache sur un espace de stockage Cloud (S3) :
  nucleaire_production.parquet  — production par réacteur
  nucleaire_jours.json          — métadonnées des jours chargés

• Déploiement : Idéal pour Streamlit Cloud (stockage persistant gratuit).
• Sécurité : Les clés sont lues depuis st.secrets.
"""

import warnings
warnings.filterwarnings("ignore")

import json
import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from entsoe import EntsoePandasClient
import s3fs # Importé pour lire/écrire le JSON sur le cloud

# ══════════════════════════════════════════════════════════════════
# 0. CONFIGURATION
# ══════════════════════════════════════════════════════════════════

API_KEY               = "c5cb3857-bc40-4f4c-a4db-088946785b4a"
COUNTRY               = "FR"
TZ                    = "Europe/Paris"
SEUIL_ON_PCT          = 5
N_COLS_SPARKLINES     = 4
BLOC_JOURS            = 30      
MAX_SHAPES            = 56     

# ── Configuration du stockage Cloud (Cloudflare R2 / S3) ───────────
# Ces valeurs doivent être configurées dans les secrets de Streamlit
# (.streamlit/secrets.toml en local, ou dans les paramètres sur Streamlit Cloud)
try:
    STORAGE_OPTIONS = {
        "key": st.secrets["cloudflare"]["R2_ACCESS_KEY_ID"],
        "secret": st.secrets["cloudflare"]["R2_SECRET_ACCESS_KEY"],
        "client_kwargs": {
            "endpoint_url": st.secrets["cloudflare"]["R2_ENDPOINT_URL"]
        }
    }
    BUCKET_NAME = st.secrets["cloudflare"]["R2_BUCKET_NAME"]
except KeyError:
    st.error("⚠️ Clés Cloudflare introuvables. Vérifiez vos secrets Streamlit.")
    st.stop()

PARQUET_URI = f"s3://{BUCKET_NAME}/nucleaire_production.parquet"
JSON_URI    = f"s3://{BUCKET_NAME}/nucleaire_jours.json"

PUISSANCE_NOMINALE_MW = {
    "BUGEY 2": 910,     "BUGEY 3": 910,     "BUGEY 4": 880,     "BUGEY 5": 880,
    "BLAYAIS 1": 910,   "BLAYAIS 2": 910,   "BLAYAIS 3": 910,   "BLAYAIS 4": 910,
    "CHINON 1": 905,    "CHINON 2": 905,    "CHINON 3": 905,    "CHINON 4": 905,
    "CRUAS 1": 915,     "CRUAS 2": 915,     "CRUAS 3": 915,     "CRUAS 4": 915,
    "DAMPIERRE 1": 890, "DAMPIERRE 2": 890, "DAMPIERRE 3": 890, "DAMPIERRE 4": 890,
    "GRAVELINES 1": 910,"GRAVELINES 2": 910,"GRAVELINES 3": 910,
    "GRAVELINES 4": 910,"GRAVELINES 5": 910,"GRAVELINES 6": 910,
    "ST LAURENT 1": 915,"ST LAURENT 2": 915,
    "TRICASTIN 1": 915, "TRICASTIN 2": 915, "TRICASTIN 3": 915, "TRICASTIN 4": 915,
    "FLAMANVILLE 1": 1310,"FLAMANVILLE 2": 1310,
    "PALUEL 1": 1330,   "PALUEL 2": 1330,   "PALUEL 3": 1330,   "PALUEL 4": 1330,
    "ST ALBAN 1": 1335, "ST ALBAN 2": 1335,
    "BELLEVILLE 1": 1310,"BELLEVILLE 2": 1310,
    "CATTENOM 1": 1300, "CATTENOM 2": 1300, "CATTENOM 3": 1300, "CATTENOM 4": 1300,
    "GOLFECH 1": 1310,  "GOLFECH 2": 1310,
    "NOGENT 1": 1310,   "NOGENT 2": 1310,
    "PENLY 1": 1320,    "PENLY 2": 1320,
    "CHOOZ 1": 1500,    "CHOOZ 2": 1500,
    "CIVAUX 1": 1495,   "CIVAUX 2": 1495,
    "FLAMANVILLE 3": 1630,
}

AUJOURDHUI    = datetime.now().date()
HIER          = AUJOURDHUI - timedelta(days=1)
_parquet_lock = threading.Lock()

# ══════════════════════════════════════════════════════════════════
# 1. EXTRACTION ENTSO-E
# ══════════════════════════════════════════════════════════════════

def _dedup_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.duplicated().any():
        df = df.T.groupby(level=0).max().T
    return df

def extraire_actual_aggregated(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        niv0 = df.columns.get_level_values(0).astype(str)
        niv1 = df.columns.get_level_values(1).astype(str)
        m1   = niv1.str.contains("Aggregated", case=False, na=False)
        m0   = niv0.str.contains("Aggregated", case=False, na=False)
        if m1.any():
            out = df.loc[:, m1].copy(); out.columns = out.columns.droplevel(1)
        elif m0.any():
            out = df.loc[:, m0].copy(); out.columns = out.columns.droplevel(0)
        else:
            out = df.copy(); out.columns = niv0
    else:
        out = df.copy()
    out.columns = [str(c) for c in out.columns]
    out = _dedup_columns(out)
    return out

# ══════════════════════════════════════════════════════════════════
# 2. CACHE CLOUD (R2 / S3)
# ══════════════════════════════════════════════════════════════════

def _get_fs():
    """Retourne le système de fichiers S3 connecté."""
    return s3fs.S3FileSystem(**STORAGE_OPTIONS)

def _load_parquet_raw() -> pd.DataFrame:
    """Lit le fichier Parquet depuis le Cloud."""
    try:
        df = pd.read_parquet(PARQUET_URI, storage_options=STORAGE_OPTIONS)
        if df.empty:
            return pd.DataFrame()
        if df.index.tz is None:
            df.index = df.index.tz_localize(TZ)
        elif str(df.index.tz) != TZ:
            df.index = df.index.tz_convert(TZ)
        return df
    except Exception:
        return pd.DataFrame()

def _load_jours_meta_raw() -> dict:
    """Lit le JSON de métadonnées depuis le Cloud."""
    fs = _get_fs()
    try:
        with fs.open(JSON_URI, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def jours_cache_dict(start: date, end: date) -> dict[date, tuple[str, int]]:
    meta      = _load_jours_meta_raw()
    if not meta:
        return {}
        
    available = sorted(date.fromisoformat(j) for j in meta)
    requested = [start + timedelta(days=i) for i in range((end - start).days + 1)]

    result: dict[date, tuple[str, int]] = {}
    ptr = 0
    n   = len(available)

    for j in requested:
        while ptr < n and available[ptr] < j:
            ptr += 1
        if ptr < n and available[ptr] == j:
            m = meta[str(j)]
            result[j] = (m["charge_ts"], m["est_complet"])
            ptr += 1

    return result

def sauvegarder_batch_en_parquet(resultats_par_jour: dict[date, pd.DataFrame]) -> None:
    """Persiste sur le Cloud en remplaçant l'ancien fichier."""
    dfs_to_add: list[pd.DataFrame] = []
    meta_updates: dict              = {}
    now_iso = datetime.now().isoformat()
    today   = datetime.now().date()

    for jour, df_wide in resultats_par_jour.items():
        if df_wide is None or df_wide.empty:
            continue
        if jour >= today:
            continue
        idx = df_wide.index
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        df_j         = df_wide.copy()
        df_j.index   = idx.tz_convert(TZ)
        df_j.columns = [str(c) for c in df_j.columns]
        df_j = _dedup_columns(df_j)
        dfs_to_add.append(df_j)
        meta_updates[str(jour)] = {"charge_ts": now_iso, "est_complet": 1}

    if not dfs_to_add:
        return

    with _parquet_lock:
        df_existing = _load_parquet_raw()
        df_new = pd.concat(dfs_to_add, axis=0)
        df_new = df_new[~df_new.index.duplicated(keep="last")]

        if not df_existing.empty:
            df_combined = pd.concat([df_existing, df_new], axis=0, join="outer")
            df_combined = df_combined[~df_combined.index.duplicated(keep="last")]
            df_combined = df_combined.sort_index()
            df_combined = _dedup_columns(df_combined)
        else:
            df_combined = df_new.sort_index()

        # Sauvegarde Parquet sur le Cloud
        df_combined.to_parquet(PARQUET_URI, storage_options=STORAGE_OPTIONS)

        # Mise à jour du JSON sur le Cloud
        meta = _load_jours_meta_raw()
        meta.update(meta_updates)
        fs = _get_fs()
        with fs.open(JSON_URI, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    charger_depuis_parquet_cache.clear()

@st.cache_data(show_spinner=False)
def charger_depuis_parquet_cache(start: date, end: date) -> pd.DataFrame:
    df_prod = _load_parquet_raw()
    if df_prod.empty:
        return pd.DataFrame()

    borne_start = pd.Timestamp(str(start), tz=TZ)
    borne_end   = pd.Timestamp(str(end) + " 23:59:59", tz=TZ)
    mask = (df_prod.index >= borne_start) & (df_prod.index <= borne_end)
    return df_prod.loc[mask].copy()

def stats_parquet() -> dict:
    meta = _load_jours_meta_raw()
    if not meta:
        return {"n": 0, "min": None, "max": None}
    jours = sorted(meta.keys())
    return {"n": len(jours), "min": jours[0], "max": jours[-1]}

def purger_periode_parquet(start: date, end: date) -> int:
    jours = [start + timedelta(days=i) for i in range((end - start).days + 1)]

    with _parquet_lock:
        meta = _load_jours_meta_raw()
        for j in jours:
            meta.pop(str(j), None)
            
        fs = _get_fs()
        with fs.open(JSON_URI, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        df_prod = _load_parquet_raw()
        if not df_prod.empty:
            borne_start = pd.Timestamp(str(start), tz=TZ)
            borne_end   = pd.Timestamp(str(end) + " 23:59:59", tz=TZ)
            mask    = (df_prod.index >= borne_start) & (df_prod.index <= borne_end)
            df_prod = df_prod[~mask]
            
            if df_prod.empty:
                try: fs.rm(PARQUET_URI)
                except Exception: pass
            else:
                df_prod.to_parquet(PARQUET_URI, storage_options=STORAGE_OPTIONS)

    charger_depuis_parquet_cache.clear()
    return len(jours)

# ══════════════════════════════════════════════════════════════════
# 3. API ENTSO-E
# ══════════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner=False)
def get_entsoe_client() -> EntsoePandasClient:
    return EntsoePandasClient(api_key=API_KEY)

def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def api_telecharger_bloc(bloc: list[date], client: EntsoePandasClient) -> dict[date, pd.DataFrame | None]:
    start_ts = pd.Timestamp(str(bloc[0])  + " 00:00", tz=TZ)
    end_ts   = pd.Timestamp(str(bloc[-1]) + " 23:59", tz=TZ)
    try:
        df_raw = client.query_generation_per_plant(
            country_code=COUNTRY, start=start_ts, end=end_ts, psr_type="B14"
        )
    except Exception:
        return {j: None for j in bloc}

    if df_raw is None or df_raw.empty:
        return {j: None for j in bloc}

    df_wide   = extraire_actual_aggregated(df_raw)
    resultats: dict[date, pd.DataFrame | None] = {}

    for jour in bloc:
        borne_s = pd.Timestamp(str(jour) + " 00:00", tz=TZ)
        borne_e = pd.Timestamp(str(jour) + " 23:59", tz=TZ)
        df_j    = df_wide[(df_wide.index >= borne_s) & (df_wide.index <= borne_e)]
        resultats[jour] = df_j if not df_j.empty else None

    return resultats

# ══════════════════════════════════════════════════════════════════
# 4. INTERFACE STREAMLIT
# ══════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="☢️ Modulation nucléaire France",
    layout="wide",
    page_icon="☢️",
)
st.title("☢️ Modulation nucléaire par réacteur — France")
st.caption(
    "Production normalisée par la puissance nominale (IAEA PRIS) · "
    "Cache : Cloudflare R2 · Données : ENTSO-E"
)

st.markdown("""
<style>
div[data-baseweb="calendar"] button[aria-selected="true"],
div[data-baseweb="calendar"] [aria-selected="true"] > button {
    background-color: #c2185b !important;
    color: #fff !important;
    border-radius: 50% !important;
}
div[data-baseweb="calendar"] [data-highlighted="true"] button,
div[data-baseweb="calendar"] div[data-highlighted="true"] button {
    background-color: #fce4ec !important;
    border-radius: 0 !important;
}
div[data-baseweb="calendar"] button:disabled {
    opacity: 0.30 !important;
    cursor: not-allowed !important;
}
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("📅 Période")

    dates = st.date_input(
        "Sélectionner la plage",
        value=[HIER - timedelta(days=6), HIER],
        min_value=date(2015, 1, 1),
        max_value=AUJOURDHUI,
        format="DD/MM/YYYY",
        help="Cliquez d'abord sur la date de début, puis sur la date de fin.",
    )

    if isinstance(dates, date):
        dates = (dates,)

    if len(dates) < 2:
        st.info("📅 Cliquez maintenant sur la date de fin.")
        st.stop()

    start_date, end_date = dates[0], dates[1]
    nb_jours = (end_date - start_date).days + 1

    st.info(f"📆 {nb_jours} jour(s) sélectionné(s)")
    if nb_jours > 31:
        st.warning("⚠️ Au-delà de 31 jours, le premier chargement peut être long.")

    MAX_WORKERS_API = st.select_slider(
        "⚡ Parallélisme API",
        options=[2, 4, 6, 8],
        value=4,
        help="Nombre de blocs téléchargés simultanément depuis ENTSO-E.",
    )

    lancer = st.button("🔄 Rafraîchir", type="primary", use_container_width=True)

    with st.expander("🗑️ Gestion du cache"):
        st.caption("Force un re-téléchargement de la période sélectionnée.")
        if st.button("Purger la période", use_container_width=True):
            n = purger_periode_parquet(start_date, end_date)
            st.toast(f"🗑️ {n} jour(s) supprimés du cache", icon="✅")

    st.markdown("---")
    info = stats_parquet()
    if info["n"] == 0:
        st.caption("📂 Cache Cloud vide — premier lancement.")
    else:
        st.caption(
            f"☁️ Cache Cloud : **{info['n']} jours**\n\n"
            f"Du {info['min']} au {info['max']}"
        )
    st.markdown(
        "**Source Pnom** : IAEA PRIS"
    )

if "premier_chargement" not in st.session_state:
    st.session_state.premier_chargement = True
    lancer = True

if not lancer:
    st.stop()

if start_date > end_date:
    st.error("La date de début doit être antérieure à la date de fin.")
    st.stop()

# ══════════════════════════════════════════════════════════════════
# 5. CHARGEMENT
# ══════════════════════════════════════════════════════════════════

def charger_periode(start: date, end: date) -> tuple[pd.DataFrame, int, int]:
    tous_les_jours = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    jours_info = jours_cache_dict(start, end)
    now = datetime.now()

    def _est_cache(j: date) -> bool:
        if j >= now.date(): return False
        return jours_info.get(j) is not None

    jours_a_fetcher = [j for j in tous_les_jours if not _est_cache(j)]
    nb_cache        = len(tous_les_jours) - len(jours_a_fetcher)
    echecs: list[tuple[str, str]] = []
    nb_ok  = 0

    if jours_a_fetcher:
        blocs  = list(_chunks(jours_a_fetcher, BLOC_JOURS))
        client = get_entsoe_client()

        barre   = st.progress(0.0, text="⚡ Téléchargement des jours manquants…")
        lock    = threading.Lock()
        counter = {"blocs": 0}

        def _fetch_bloc(bloc: list[date]) -> dict[date, pd.DataFrame | None]:
            return api_telecharger_bloc(bloc, client)

        def _process_resultats(resultats: dict[date, pd.DataFrame | None], bloc: list[date]) -> None:
            nonlocal nb_ok
            batch = {j: df for j, df in resultats.items() if df is not None}
            fails = [j for j, df in resultats.items() if df is None]
            if batch:
                sauvegarder_batch_en_parquet(batch)
                nb_ok += len(batch)
            for j in fails:
                echecs.append((str(j), "Aucune donnée retournée par l'API"))
            with lock:
                counter["blocs"] += 1
                pct  = counter["blocs"] / len(blocs)
                done = counter["blocs"] * BLOC_JOURS
                barre.progress(pct, text=f"⚡ ~{min(done, len(jours_a_fetcher))}/{len(jours_a_fetcher)} jours traités…")

        if len(blocs) == 1:
            resultats = _fetch_bloc(blocs[0])
            _process_resultats(resultats, blocs[0])
        else:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS_API) as ex:
                futures = {ex.submit(_fetch_bloc, bloc): bloc for bloc in blocs}
                try:
                    for fut in as_completed(futures, timeout=300):
                        try:
                            _process_resultats(fut.result(timeout=120), futures[fut])
                        except Exception as exc:
                            bloc = futures[fut]
                            echecs.extend([(str(j), str(exc)) for j in bloc])
                            with lock:
                                counter["blocs"] += 1
                                barre.progress(counter["blocs"] / len(blocs))
                except Exception:
                    for fut in futures:
                        fut.cancel()
                    echecs += [(str(j), "Timeout — ENTSO-E n'a pas répondu") for fut in futures if not fut.done() for j in futures[fut]]

        barre.empty()
        if echecs:
            with st.expander(f"⚠️ {len(echecs)} jour(s) en erreur"):
                for j, err in echecs:
                    st.write(f"**{j}** : {err}")

        charger_depuis_parquet_cache.clear()

    df     = charger_depuis_parquet_cache(start, end)
    nb_api = nb_ok
    return df, nb_cache, nb_api

with st.spinner("⏳ Chargement des données…"):
    try:
        df_brut, nb_cache, nb_api = charger_periode(start_date, end_date)
    except Exception as exc:
        st.error(f"Erreur : {exc}")
        st.stop()

if df_brut is None or df_brut.empty:
    st.error("Aucune donnée disponible pour cette période.")
    st.stop()

st.success(
    f"✅ Données chargées — {start_date} → {end_date} · "
    f"☁️ {nb_cache} jour(s) depuis le cache · "
    f"🌐 {nb_api} jour(s) téléchargé(s) depuis l'API"
)

# ══════════════════════════════════════════════════════════════════
# 6. TRAITEMENT
# ══════════════════════════════════════════════════════════════════

df_nuc = extraire_actual_aggregated(df_brut)
df_nuc = df_nuc.dropna(axis=1, how="all")
df_nuc = _dedup_columns(df_nuc)
if df_nuc.columns.duplicated().any():
    df_nuc = df_nuc.T.groupby(level=0).max().T

if nb_jours > 60:
    freq = "3h"
elif nb_jours > 31:
    freq = "2h"
else:
    freq = "1h"

df_nuc = df_nuc.resample(freq).mean().ffill().fillna(0)
df_nuc = df_nuc[sorted(df_nuc.columns)]

if df_nuc.empty or df_nuc.shape[1] == 0:
    st.error("Aucune donnée disponible après traitement.")
    st.stop()

reacteurs     = df_nuc.columns.tolist()
serie_pnom    = pd.Series({r: PUISSANCE_NOMINALE_MW.get(r, max(df_nuc[r].max(), 900.0)) for r in reacteurs})
df_taux       = (df_nuc.div(serie_pnom) * 100).clip(upper=105)
taux_derniere = df_taux.iloc[-1]
prod_derniere = df_nuc.iloc[-1]
reacteurs_on  = int((taux_derniere >= SEUIL_ON_PCT).sum())
reacteurs_off = int((taux_derniere < SEUIL_ON_PCT).sum())
taux_moyen    = taux_derniere[taux_derniere >= SEUIL_ON_PCT].mean()

# ══════════════════════════════════════════════════════════════════
# 7. MÉTRIQUES
# ══════════════════════════════════════════════════════════════════

st.markdown("---")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("☢️ Production totale",       f"{prod_derniere.sum():,.0f} MW")
c2.metric("✅ En marche",               f"{reacteurs_on} réacteurs")
c3.metric("🔴 Arrêtés / < 5 %",        f"{reacteurs_off} réacteurs")
c4.metric("📊 Taux de charge moyen",   f"{taux_moyen:.1f} %")
c5.metric("⚡ Puissance nominale parc", f"{serie_pnom.sum() / 1e3:.1f} GW")
st.markdown("---")

# ══════════════════════════════════════════════════════════════════
# 8. HEATMAP
# ══════════════════════════════════════════════════════════════════

st.subheader("🔲 Heatmap — Taux de charge par réacteur (% Pnom)")
st.caption("🟢 Vert = puissance nominale · ⚫ Noir = arrêt · 🟡 intermédiaire = modulation")

COLORSCALE = [
    [0.00, "rgb(5,5,5)"],    [0.04, "rgb(40,5,5)"],
    [0.15, "rgb(120,20,0)"], [0.30, "rgb(180,60,0)"],
    [0.45, "rgb(200,120,0)"],[0.60, "rgb(210,190,0)"],
    [0.75, "rgb(170,210,30)"],[0.88, "rgb(80,200,40)"],
    [0.95, "rgb(30,220,60)"], [0.99, "rgb(10,230,70)"],
    [1.00, "rgb(0,255,80)"],
]

df_plot_heat = df_taux.resample("2h").mean() if nb_jours > 31 else df_taux

fig_heatmap = go.Figure(go.Heatmap(
    z=df_plot_heat[reacteurs].T.values,
    x=df_plot_heat.index,
    y=reacteurs,
    colorscale=COLORSCALE, zmin=0, zmax=100, hoverongaps=False,
    hovertemplate="%{y}<br>%{x}<br>%{z:.1f} % Pnom<extra></extra>",
    colorbar=dict(title="% Pnom", ticksuffix=" %", tickvals=[0, 25, 50, 75, 100], tickfont=dict(size=10)),
))
fig_heatmap.update_layout(
    xaxis_title="",
    yaxis=dict(tickfont=dict(size=10), autorange="reversed"),
    template="plotly_dark",
    height=max(420, len(reacteurs) * 14),
    margin=dict(l=140, r=90, t=20, b=40),
)
st.plotly_chart(fig_heatmap, use_container_width=True, theme=None)

# ══════════════════════════════════════════════════════════════════
# 9. SPARKLINES
# ══════════════════════════════════════════════════════════════════

st.subheader("📈 Courbes individuelles — Taux de charge par réacteur")
st.caption("🟢 Vert = en marche · 🔴 Rouge = arrêté · Axe Y = % Pnom (IAEA PRIS)")

df_plot_spark = df_taux.resample("2h").mean() if nb_jours > 31 else df_taux

n_rows_spark = max(1, math.ceil(len(reacteurs) / N_COLS_SPARKLINES))
titres       = [f"{r}<br>{serie_pnom[r]:.0f} MW" for r in reacteurs]

fig_spark = make_subplots(
    rows=n_rows_spark, cols=N_COLS_SPARKLINES,
    subplot_titles=titres, shared_xaxes=True,
    vertical_spacing=0.03, horizontal_spacing=0.06,
)

for idx, reacteur in enumerate(reacteurs):
    row       = idx // N_COLS_SPARKLINES + 1
    col       = idx % N_COLS_SPARKLINES + 1
    serie_pct = df_plot_spark[reacteur]
    en_marche = serie_pct.iloc[-1] >= SEUIL_ON_PCT
    couleur   = "#00C853" if en_marche else "#E53935"
    fill_col  = "rgba(0,200,83,0.15)" if en_marche else "rgba(229,57,53,0.15)"
    fig_spark.add_trace(go.Scatter(
        x=serie_pct.index, y=serie_pct.values,
        mode="lines", line=dict(color=couleur, width=1.2),
        fill="tozeroy", fillcolor=fill_col,
        name=reacteur, showlegend=False,
        hovertemplate="%{x}<br>%{y:.1f} % Pnom<extra>" + reacteur + "</extra>",
    ), row=row, col=col)

shapes_hline = []
for idx in range(min(len(reacteurs), MAX_SHAPES)):
    ax_idx = idx + 1
    shapes_hline.append(dict(
        type="line", x0=0, x1=1, y0=100, y1=100,
        xref="x domain" if ax_idx == 1 else f"x{ax_idx} domain",
        yref="y"        if ax_idx == 1 else f"y{ax_idx}",
        line=dict(dash="dot", color="rgba(255,255,255,0.2)", width=0.8),
    ))

fig_spark.update_layout(
    template="plotly_dark", height=max(800, n_rows_spark * 200),
    hovermode="closest", margin=dict(l=30, r=20, t=60, b=20), shapes=shapes_hline,
)
fig_spark.update_annotations(font_size=9)
fig_spark.update_xaxes(showticklabels=False, showspikes=False, showgrid=False)
fig_spark.update_yaxes(
    showticklabels=True, ticksuffix="%", nticks=3,
    tickfont=dict(size=9, color="#CCCCCC"),
    gridcolor="rgba(180,180,180,0.3)", gridwidth=0.5,
    showgrid=True, zeroline=False, rangemode="tozero", showspikes=False,
)
st.plotly_chart(fig_spark, use_container_width=True, theme=None)

# ══════════════════════════════════════════════════════════════════
# 10. TABLEAU & TÉLÉCHARGEMENT
# ══════════════════════════════════════════════════════════════════

with st.expander("📋 Tableau — taux de charge par réacteur (dernière valeur)"):
    df_table = pd.DataFrame({
        "Pnom (MWe)"         : serie_pnom,
        "Production (MW)"    : prod_derniere.round(0),
        "Taux de charge (%)": taux_derniere.round(1),
        "État"               : taux_derniere.apply(
            lambda x: "✅ En marche" if x >= SEUIL_ON_PCT else "🔴 Arrêté"
        ),
    }).sort_values("Taux de charge (%)", ascending=False)
    st.dataframe(df_table, use_container_width=True)

with st.expander("📋 Télécharger les données (taux de charge %)"):
    csv = df_taux.to_csv().encode("utf-8")
    st.download_button(
        "⬇️ CSV — taux de charge horaire par réacteur", csv,
        file_name=f"modulation_nucleaire_FR_{start_date}_{end_date}.csv",
        mime="text/csv",
    )
