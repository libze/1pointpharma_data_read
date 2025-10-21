import re
import os
import json
import pickle
import glob
from typing import List, Union
import pandas as pd
import requests
import io
import msal

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

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

# ------------------------------------------------
# Device-code delegated auth for SharePoint (IRM-friendly)
# ------------------------------------------------
def get_sp_user_token_device_code(tenant_id: str, client_id: str, sp_host: str) -> str:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.PublicClientApplication(client_id=client_id, authority=authority)

    # ✅ request ONLY the SharePoint resource-specific scope here
    scopes = [f"{sp_host}/AllSites.Read"]   # e.g. "https://1pointpharma.sharepoint.com/AllSites.Read"

    # Try cached account first (silent)
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(scopes=scopes, account=accounts[0])
        if result and "access_token" in result:
            return result["access_token"]

    # Device code flow
    dc = app.initiate_device_flow(scopes=scopes)
    if "user_code" not in dc:
        raise RuntimeError(f"Failed to start device flow: {dc}")

    print("\n=== Device Code Sign-in ===")
    print(dc["message"])
    result = app.acquire_token_by_device_flow(dc)
    if "access_token" not in result:
        raise RuntimeError(f"Auth failed: {result.get('error_description') or result}")

    return result["access_token"]


def read_sharepoint_excel_delegated(
    site_url: str,
    server_relative_path: str,
    *,
    sheet_name: str | int | None = None,
    storage_cache_dir: str = CACHE_DIR,
) -> pd.DataFrame:
    """
    IRM-friendly: uses a **user** token (delegated) via device-code to fetch the file.
    """
    # simple content cache (optional)
    safe_name = server_relative_path.replace("/", "__")
    cache_path = os.path.join(storage_cache_dir, f"SP__{safe_name}.pkl")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass

    access_token = get_sp_user_token_device_code(TENANT_ID, CLIENT_ID, SP_HOST)

    # Use SharePoint REST download (/_api/web/getfilebyserverrelativeurl)/$value
    # NOTE: needs the full server-relative path starting with /sites/...
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

    mem = io.BytesIO(resp.content)
    df = pd.read_excel(mem, sheet_name=sheet_name, engine="openpyxl")

    os.makedirs(storage_cache_dir, exist_ok=True)
    with open(cache_path, "wb") as f:
        pickle.dump(df, f)
    return df

# ------------------------------------------------
# Your existing cache helpers
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
    return pd.read_excel(src_path, engine="openpyxl", **read_kwargs)

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
def read_data(files=None, force_reload: bool = False, return_dict: bool = False):
    """
    files=None: reads ALL local Excel files under Data/ (cached).
    Automatically replaces the first (sourcing) file with the SharePoint
    version from 'Sourcing and quoting date_v6.xlsx' by default.

    Falls back to the local file if SharePoint fetch fails.
    """
    if files is None:
        dfs = cached_read_excel(DATA_DIR, force_reload=force_reload)

        # Try delegated SharePoint for sourcing (won’t require removing IRM)
        try:
            sp_sourcing = read_sharepoint_excel_delegated(
                site_url=f"{SP_HOST}/sites/Sales",
                server_relative_path=SOURCING_SP_SERVER_PATH,
            )
            dfs[0] = sp_sourcing  # your pipeline expects sourcing first
        except Exception as e:
            print(f"[WARN] SharePoint delegated sourcing not loaded ({e}); using local sourcing file.")

        if return_dict:
            mapping = {}
            excel_paths = []
            for pat in EXCEL_PATTERNS:
                excel_paths.extend(glob.glob(os.path.join(DATA_DIR, pat)))
            excel_paths = sorted(set(excel_paths), key=lambda p: _preferred_order_key(os.path.basename(p)))
            for p, df in zip(excel_paths, dfs):
                mapping[os.path.basename(p)] = df
            mapping["(SharePoint) Sourcing and quoting date_v6.xlsx"] = dfs[0]
            return mapping
        return dfs

    if isinstance(files, str):
        files = [files]
    out = []
    for f in files:
        src = f if os.path.sep in f else os.path.join(DATA_DIR, f)
        out.append(cached_read_excel(src, force_reload=force_reload))
    return out

# ------------------------------------------------
# Input selection helpers (unchanged)
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
                   refs: str | None = None) -> pd.DataFrame:
    sourcing_path = os.path.join(DATA_DIR, "Sourcing and quoting date_v4.xlsx")
    df = cached_read_excel(sourcing_path, cache_dir=CACHE_DIR, force_reload=force_reload)

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
            print("[WARN] No rows matched the given Ref values; falling back to tail.")
            return df.tail(n_rows)
        return picked
    return df.tail(n_rows)

# ------------------------------------------------
# __main__ test
# ------------------------------------------------
if __name__ == "__main__":
    print("=== Testing SharePoint sourcing (delegated, IRM-friendly) ===")
    try:
        df_sp = read_sharepoint_excel_delegated(
            site_url=SOURCING_SP_SITE_URL,
            server_relative_path=SOURCING_SP_SERVER_PATH,
        )
        df_sp = df_sp["Sales"]
        print("SharePoint sourcing OK:", df_sp.shape)
    except Exception as e:
        print("SharePoint sourcing FAILED:", e)

    print("\n=== Testing read_data() wrapper ===")
    try:
        data_list = read_data()
        for i, df in enumerate(data_list):
            print(f"[{i}] shape:", getattr(df, "shape", None))
    except Exception as e:
        print("read_data FAILED:", e)

    print("\n=== Testing get_input_rows() (tail fallback) ===")
    try:
        tail = get_input_rows(testing=True, n_rows=3)
        print(tail.head(3))
    except Exception as e:
        print("get_input_rows FAILED:", e)
