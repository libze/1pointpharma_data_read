# Reader.py
import re
import os
import json
import pickle
import glob
from typing import List, Union, Optional
import pathlib
import time
import pandas as pd
import requests
import io
import msal
from msal import PublicClientApplication, SerializableTokenCache

DATA_DIR  = "Data"
CACHE_DIR = "Cache"

EXCEL_PATTERNS = ("*.xlsx", "*.xls", "*.xlsm")

# === CONFIG: SharePoint location for SOURCING ===
SP_HOST = "https://1pointpharma.sharepoint.com"
SOURCING_SP_SITE_URL = f"{SP_HOST}/sites/Sales"
SOURCING_SP_SERVER_PATH = (
    "/sites/Sales/Shared Documents/Unlicensed Medicine/Sourcing og data management/"
    "Sourcing and quoting date_v6.xlsx"
)

# === AUTH CONFIG (env vars) ===
TENANT_ID   = os.getenv("SP_TENANT_ID")   or "e4dd8020-0b04-4a72-b071-f9798b651590"
CLIENT_ID   = os.getenv("SP_CLIENT_ID")   or "a91e37f2-4a4d-4b6d-92d5-84c603c9de84"
CLIENT_SECRET = os.getenv("SP_CLIENT_SECRET")  # not used for delegated but kept for app-only fallbacks

# Token cache file (persist across runs so you only login once)
os.makedirs(CACHE_DIR, exist_ok=True)
TOKEN_CACHE_PATH = os.path.join(CACHE_DIR, "msal_token_cache.bin")

# Prefer a sheet named "Sales"; otherwise, pick first sheet
SHEET_PREFERENCES = ("Sales",)

def _ensure_dataframe(x):
    """
    If x is a dict-of-DataFrames (multiple Excel sheets), pick a single DataFrame.
    Preference order: exact sheet name in SHEET_PREFERENCES (case-insensitive), else first sheet.
    """
    if isinstance(x, dict):
        # Try preferred names
        for pref in SHEET_PREFERENCES:
            for k in x.keys():
                if str(k).strip().lower() == pref.lower():
                    return x[k]
        # Fallback to first sheet deterministically
        first_key = next(iter(x.keys()))
        return x[first_key]
    return x

def _row_has_content_excluding_ref(row: pd.Series) -> bool:
    """
    A row is considered non-empty only if at least one cell
    outside of the 'Ref' column has non-blank content.
    """
    for col, val in row.items():
        if col == "Ref":
            continue
        if pd.notna(val) and str(val).strip():
            return True
    return False

def _drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows that are entirely empty OR that only contain a 'Ref' value.
    """
    if df is None or df.empty:
        return df
    # Drop fully-NaN rows
    df = df.dropna(how="all")
    # Keep only rows with content outside 'Ref'
    mask = df.apply(_row_has_content_excluding_ref, axis=1)
    return df.loc[mask].reset_index(drop=True)

# ------------------------------------------------
# Device-code delegated auth for SharePoint WITH PERSISTED TOKEN CACHE
# ------------------------------------------------

def _load_token_cache() -> SerializableTokenCache:
    cache = SerializableTokenCache()
    if os.path.exists(TOKEN_CACHE_PATH):
        # READ AS TEXT
        with open(TOKEN_CACHE_PATH, "r", encoding="utf-8") as f:
            cache.deserialize(f.read())
    return cache

def _save_token_cache(cache: SerializableTokenCache) -> None:
    if cache.has_state_changed:
        # WRITE AS TEXT
        with open(TOKEN_CACHE_PATH, "w", encoding="utf-8") as f:
            f.write(cache.serialize())

def sign_out_sharepoint():
    """Optional helper if you want to force re-login next run."""
    try:
        if os.path.exists(TOKEN_CACHE_PATH):
            os.remove(TOKEN_CACHE_PATH)
            print("SharePoint token cache removed.")
    except Exception as e:
        print(f"[WARN] Could not remove token cache: {e}")

def get_sp_user_token_device_code(tenant_id: str, client_id: str, sp_host: str) -> str:
    """
    Login once with device code if needed; afterwards reuse/refresh silently.
    Cache is persisted under Cache/msal_token_cache.bin.
    """
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes = [f"{sp_host}/AllSites.Read"]

    cache = _load_token_cache()
    app = PublicClientApplication(client_id=client_id, authority=authority, token_cache=cache)

    # 1) Try silent first (also uses refresh tokens if available)
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(scopes=scopes, account=accounts[0])
        if result and "access_token" in result:
            _save_token_cache(cache)
            return result["access_token"]

    # 2) First-time (or cache invalid): device flow once
    dc = app.initiate_device_flow(scopes=scopes)
    if "user_code" not in dc:
        raise RuntimeError(f"Failed to start device flow: {dc}")

    print("\n=== Device Code Sign-in ===")
    print(dc["message"])
    result = app.acquire_token_by_device_flow(dc)  # blocks until auth completes

    if "access_token" not in result:
        raise RuntimeError(f"Auth failed: {result.get('error_description') or result}")

    _save_token_cache(cache)
    return result["access_token"]

def _download_sp_file(site_url: str, server_relative_path: str, access_token: str) -> bytes:
    download_url = (f"{site_url}/_api/web/GetFileByServerRelativeUrl(@u)/$value"
                    f"?@u='{requests.utils.quote(server_relative_path, safe='')}'")
    resp = requests.get(
        download_url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json;odata=nometadata"
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.content

def read_sharepoint_excel_delegated(
    site_url: str,
    server_relative_path: str,
    *,
    sheet_name: str | int | None = None,
    storage_cache_dir: str = CACHE_DIR,
    force_reload: bool = True,       # default: always fresh each run
    use_disk_cache: bool = False,    # keep OFF so we always update
) -> pd.DataFrame:
    """
    IRM-friendly: uses a user token (delegated) via device-code to fetch the file.
    We persist the MSAL cache so login happens once; subsequent runs are silent.

    If use_disk_cache=False (default), this function:
      - does NOT read/write any pickle
      - fetches LIVE content from SharePoint each call (force_reload is ignored)

    If use_disk_cache=True, it will use a pickle cache unless force_reload=True.
    """
    safe_name = server_relative_path.replace("/", "__")
    cache_path = os.path.join(storage_cache_dir, f"SP__{safe_name}.pkl")

    # ---- READ DISK CACHE only if explicitly enabled ----
    if use_disk_cache and not force_reload and os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass

    # Always get a valid token (silent if cached), then live download
    access_token = get_sp_user_token_device_code(TENANT_ID, CLIENT_ID, SP_HOST)
    content = _download_sp_file(site_url, server_relative_path, access_token)

    mem = io.BytesIO(content)
    df = pd.read_excel(mem, sheet_name=sheet_name, engine="openpyxl")
    df = _ensure_dataframe(df)
    df = _drop_empty_rows(df)  # <-- remove empty / Ref-only rows

    # ---- WRITE DISK CACHE only if explicitly enabled ----
    if use_disk_cache:
        os.makedirs(storage_cache_dir, exist_ok=True)
        try:
            with open(cache_path, "wb") as f:
                pickle.dump(df, f)
        except Exception:
            # Don't fail the run just because caching failed
            pass

    return df

# ------------------------------------------------
# Cache helpers (local files only)
# ------------------------------------------------
def _meta_path(cache_path: str) -> str:
    return cache_path + ".meta.json"

def _fingerprint(path: str) -> dict:
    st = os.stat(path)
    return {"size": st.st_size, "mtime": st.st_mtime}

def _load_if_fresh(cache_path: str, src_path: str, force_reload: bool):
    if not os.path.exists(cache_path) or force_reload:
        return None
    try:
        with open(_meta_path(cache_path), "r", encoding="utf-8") as f:
            meta = json.load(f)
        fresh = (meta.get("fingerprint") == _fingerprint(src_path))
        if not fresh:
            return None
        with open(cache_path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None

def _write_cache(cache_path: str, df: pd.DataFrame, src_path: str) -> None:
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "wb") as f:
        pickle.dump(df, f)
    with open(_meta_path(cache_path), "w", encoding="utf-8") as f:
        json.dump({"fingerprint": _fingerprint(src_path)}, f)

# ------------------------------------------------
# Local Excel reader
# ------------------------------------------------
def _read_excel_with_rules(src_path: str, **kwargs) -> pd.DataFrame:
    read_kwargs = dict(kwargs)
    fname = os.path.basename(src_path).lower()
    if "legemiddel" in fname and "skiprows" not in read_kwargs:
        read_kwargs["skiprows"] = 2
    df = pd.read_excel(src_path, engine="openpyxl", **read_kwargs)
    return _drop_empty_rows(df)  # <-- remove empty / Ref-only rows

def _preferred_order_key(basename: str) -> tuple:
    name = basename.lower()
    def starts(s): return name.startswith(s)
    priority = 999
    if starts("sourcing and quoting"):
        priority = 0
    elif starts("legemiddel"):
        priority = 1
    elif starts("special access list"):
        priority = 2
    elif starts("product catalog"):
        priority = 3
    return (priority, name)

def cached_read_excel(path: str,
                      cache_dir: str = CACHE_DIR,
                      force_reload: bool = False,
                      **kwargs) -> Union[pd.DataFrame, List[pd.DataFrame]]:
    if os.path.isdir(path):
        excel_files = []
        for pat in EXCEL_PATTERNS:
            excel_files.extend(glob.glob(os.path.join(path, pat)))
        excel_files = sorted(set(excel_files), key=lambda p: _preferred_order_key(os.path.basename(p)))

        if not excel_files:
            raise FileNotFoundError(f"No Excel files found in directory: {path}")

        dfs = []
        for src in excel_files:
            base = os.path.basename(src)
            cache_path = os.path.join(cache_dir, base + ".pkl")

            df = _load_if_fresh(cache_path, src, force_reload)
            if df is None:
                df = _read_excel_with_rules(src, **kwargs)
                _write_cache(cache_path, df, src)
            dfs.append(df)
        return dfs

    if not os.path.exists(path):
        alt = os.path.join(DATA_DIR, path)
        if os.path.exists(alt):
            path = alt
        else:
            raise FileNotFoundError(f"No such file: {path}")

    base = os.path.basename(path)
    cache_path = os.path.join(cache_dir, base + ".pkl")

    df = _load_if_fresh(cache_path, path, force_reload)
    if df is None:
        df = _read_excel_with_rules(path, **kwargs)
        _write_cache(cache_path, df, path)
    return df

# ------------------------------------------------
# Convenience wrappers
# ------------------------------------------------
def _build_local_mapping_and_paths(force_reload: bool):
    """Load local Data/ Excels and return (dfs, excel_paths)."""
    dfs = cached_read_excel(DATA_DIR, force_reload=force_reload)
    excel_paths = []
    for pat in EXCEL_PATTERNS:
        excel_paths.extend(glob.glob(os.path.join(DATA_DIR, pat)))
    excel_paths = sorted(set(excel_paths), key=lambda p: _preferred_order_key(os.path.basename(p)))
    return dfs, excel_paths

def read_data(files=None, force_reload: bool = False, return_dict: bool = False):
    """
    files=None: reads ALL local Excel files under Data/ (cached).
    Automatically replaces the first (sourcing) file with the SharePoint
    version from 'Sourcing and quoting date_v6.xlsx' by default.

    Falls back to the local file if SharePoint fetch fails.
    """
    if files is None:
        dfs, excel_paths = _build_local_mapping_and_paths(force_reload)

        # Try delegated SharePoint for sourcing (preferred)
        sp_loaded = False
        sp_sourcing = None
        try:
            sp_sourcing = read_sharepoint_excel_delegated(
                site_url=SOURCING_SP_SITE_URL,
                server_relative_path=SOURCING_SP_SERVER_PATH,
                force_reload=True,       # always live
                use_disk_cache=False,
            )
            sp_sourcing = _ensure_dataframe(sp_sourcing)
            dfs[0] = sp_sourcing
            sp_loaded = True
        except Exception as e:
            print(f"[WARN] SharePoint delegated sourcing not loaded ({e}); using local sourcing file.")

        if return_dict:
            # Build mapping of local files first
            mapping = {}
            for p, df in zip(excel_paths, dfs):
                mapping[os.path.basename(p)] = df

            if sp_loaded and sp_sourcing is not None:
                # Remove any local 'sourcing and quoting' entries to avoid accidental use
                sourcing_keys = [k for k in list(mapping.keys()) if "sourcing and quoting" in k.lower()]
                for k in sourcing_keys:
                    mapping.pop(k, None)

                # Insert SharePoint entry FIRST to bias iteration / first-match lookups
                ordered = {"(SharePoint) Sourcing and quoting date_v6.xlsx": sp_sourcing}
                ordered.update(mapping)
                return ordered

            # SP not loaded; return locals
            return mapping

        # list mode: already replaced dfs[0] above if SP loaded
        return dfs

    # Explicit file mode
    if isinstance(files, str):
        files = [files]
    out = []
    for f in files:
        src = f if os.path.sep in f else os.path.join(DATA_DIR, f)
        out.append(cached_read_excel(src, force_reload=force_reload))
    return out

# ------------------------------------------------
# Input selection helpers
# ------------------------------------------------
def _parse_ref_selector(text: str) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    out: list[str] = []
    for chunk in text.split(","):
        c = chunk.strip()
        if not c:
            continue
        m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", c)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a <= b:
                out.extend(str(i) for i in range(a, b + 1))
            else:
                out.extend(str(i) for i in range(a, b - 1, -1))
        else:
            if re.match(r"^\d+$", c):
                out.append(c)
    seen = set()
    ordered = []
    for r in out:
        if r not in seen:
            seen.add(r)
            ordered.append(r)
    return ordered

def _normalize_ref_series(s: pd.Series) -> pd.Series:
    def _to_ref_str(v):
        if pd.isna(v):
            return ""
        try:
            fv = float(v)
            if fv.is_integer():
                return str(int(fv))
        except Exception:
            pass
        return str(v).strip()
    return s.map(_to_ref_str)

def get_input_rows(testing: bool = False,
                   force_reload: bool = False,
                   n_rows: int = 5,
                   refs: str | None = None,
                   sourcing_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Source the input rows from SharePoint by default; fall back to local v4.
    If `sourcing_df` is provided, use it directly (no SharePoint call).
    """
    df = None

    # Prefer the already-loaded SharePoint DF from read_data()
    if sourcing_df is not None:
        df = _ensure_dataframe(sourcing_df)
    else:
        # Try SharePoint first (preferred) — always fresh
        try:
            df = read_sharepoint_excel_delegated(
                site_url=SOURCING_SP_SITE_URL,
                server_relative_path=SOURCING_SP_SERVER_PATH,
                force_reload=True,
                use_disk_cache=False,
            )
        except Exception as e:
            print(f"[WARN] Could not read sourcing from SharePoint for input selection ({e}); "
                  f"falling back to local v4.")

        # Fallback to local v4 if SP not available
        if df is None:
            sourcing_path = os.path.join(DATA_DIR, "Sourcing and quoting date_v4.xlsx")
            df = cached_read_excel(sourcing_path, cache_dir=CACHE_DIR, force_reload=force_reload)

    df = _ensure_dataframe(df)  # ensure single DataFrame
    df = _drop_empty_rows(df)   # ensure we ignore Ref-only/empty rows

    if refs is None and not testing:
        user_in = input("Enter Ref numbers/ranges (e.g. 25-32, 41) or press Enter for tail: ").strip()
    else:
        user_in = refs or ""

    selected_refs = _parse_ref_selector(user_in)
    if selected_refs and "Ref" in df.columns:
        ref_col = _normalize_ref_series(df["Ref"])
        picked = df.loc[ref_col.isin(selected_refs)].copy()
        picked["_order"] = pd.Categorical(_normalize_ref_series(picked["Ref"]),
                                          categories=selected_refs, ordered=True)
        picked = picked.sort_values("_order").drop(columns=["_order"])
        if picked.empty:
            print("[WARN] No rows matched the given Ref values; falling back to last non-empty rows.")
        else:
            return picked.reset_index(drop=True)

    # Fallback: last N non-empty rows (ignoring Ref-only rows)
    df_nonempty = df[df.apply(_row_has_content_excluding_ref, axis=1)].reset_index(drop=True)

    if df_nonempty.empty:
        print("[WARN] All rows empty (except Ref); returning empty DataFrame.")
        return df_nonempty

    return df_nonempty.tail(n_rows).reset_index(drop=True)

# ------------------------------------------------
# __main__ test
# ------------------------------------------------
if __name__ == "__main__":
    print("=== Testing SharePoint sourcing (delegated, IRM-friendly) ===")
    try:
        df_sp = read_sharepoint_excel_delegated(
            site_url=SOURCING_SP_SITE_URL,
            server_relative_path=SOURCING_SP_SERVER_PATH,
            force_reload=True,
        )
        print("SharePoint sourcing OK:", getattr(df_sp, "shape", None))
    except Exception as e:
        print("SharePoint sourcing FAILED:", e)

    print("\n=== Testing read_data() wrapper ===")
    try:
        data_list = read_data(force_reload=True)
        for i, df in enumerate(data_list):
            print(f"[{i}] shape:", getattr(df, "shape", None))
    except Exception as e:
        print("read_data FAILED:", e)

    print("\n=== Testing get_input_rows() (tail fallback) ===")
    try:
        # Reuse the already loaded SP df to avoid a second network call
        sp_only = read_sharepoint_excel_delegated(
            site_url=SOURCING_SP_SITE_URL,
            server_relative_path=SOURCING_SP_SERVER_PATH,
            force_reload=True,
        )
        tail = get_input_rows(testing=True, n_rows=3, force_reload=True, sourcing_df=sp_only)
        print(tail.head(3))
    except Exception as e:
        print("get_input_rows FAILED:", e)
