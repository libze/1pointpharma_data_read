import os
import json
import pickle
import glob
from typing import List, Union
import pandas as pd

DATA_DIR  = "Data"
CACHE_DIR = "Cache"

EXCEL_PATTERNS = ("*.xlsx", "*.xls", "*.xlsm")


# ---------- helpers: cache meta ----------
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


# ---------- main reader ----------
def _read_excel_with_rules(src_path: str, **kwargs) -> pd.DataFrame:
    # special-case: legemiddel files need skiprows=2 unless user overrides
    read_kwargs = dict(kwargs)
    fname = os.path.basename(src_path).lower()
    if "legemiddel" in fname and "skiprows" not in read_kwargs:
        read_kwargs["skiprows"] = 2
    return pd.read_excel(src_path, **read_kwargs)

def _preferred_order_key(basename: str) -> tuple:
    """Stable ordering so your indices [0..4] match previous behavior."""
    name = basename.lower()
    def starts(s): return name.startswith(s)
    priority = 999
    if starts("sourcing and quoting"):
        priority = 0
    elif starts("legemiddel"):
        priority = 1
    elif starts("topp 500"):
        priority = 2
    elif starts("special access list"):
        priority = 3
    elif starts("product catalog"):
        priority = 4
    return (priority, name)

def cached_read_excel(path: str,
                      cache_dir: str = CACHE_DIR,
                      force_reload: bool = False,
                      **kwargs) -> Union[pd.DataFrame, List[pd.DataFrame]]:
    """
    - If `path` is a file: return a single DataFrame (cached).
    - If `path` is a directory: read/cache ALL Excel files inside and return
      a list of DataFrames in a stable order (see _preferred_order_key).
    """
    if os.path.isdir(path):
        # collect excel files
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

    # single file
    if not os.path.exists(path):
        # if a bare filename was passed, look under Data/
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


# ---------- convenience wrappers ----------
def read_data(files=None, force_reload: bool = False, return_dict: bool = False):
    """
    - files=None: read ALL Excel files in Data/ (cached). Returns list (stable order) or dict if return_dict=True.
    - files=str or list: read those specific files from Data/ (cached). Returns list.
    """
    if files is None:
        dfs = cached_read_excel(DATA_DIR, force_reload=force_reload)
        if return_dict:
            # map by basename so you can pick by name if you prefer
            mapping = {}
            # reconstruct names in the same order
            excel_paths = []
            for pat in EXCEL_PATTERNS:
                excel_paths.extend(glob.glob(os.path.join(DATA_DIR, pat)))
            excel_paths = sorted(set(excel_paths), key=lambda p: _preferred_order_key(os.path.basename(p)))
            for p, df in zip(excel_paths, dfs):
                mapping[os.path.basename(p)] = df
            return mapping
        return dfs

    # explicit list behavior
    if isinstance(files, str):
        files = [files]
    out = []
    for f in files:
        # prefer relative to Data/ unless it's already a path
        src = f if os.path.sep in f else os.path.join(DATA_DIR, f)
        out.append(cached_read_excel(src, force_reload=force_reload))
    return out


# reader.py
def get_input_rows(testing: bool = False, force_reload: bool = False, n_rows: int = 3) -> pd.DataFrame:
    """
    Returns the trailing n_rows from 'Sourcing and quoting date_v4.xlsx' (cached).
    If testing=False and n_rows is not provided, will prompt the user.
    """
    if not testing and (n_rows is None or n_rows <= 0):
        n_rows = int(input("How many rows should be taken into account?\n"))
    if n_rows is None or n_rows <= 0:
        n_rows = 3

    src = os.path.join(DATA_DIR, "Sourcing and quoting date_v4.xlsx")
    df = cached_read_excel(src, force_reload=force_reload)
    return df.tail(n_rows)
