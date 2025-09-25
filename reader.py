import re
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



def _parse_ref_selector(text: str) -> list[str]:
    """
    Parse strings like '25-32, 40, 42-43' into a list of ref *strings* preserving order.
    Accepts single numbers and ranges. Ignores empty tokens.
    """
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
                out.extend(str(i) for i in range(a, b - 1, -1))  # tolerate reversed ranges
        else:
            m2 = re.match(r"^\d+$", c)
            if m2:
                out.append(c)
            # else: silently ignore non-numeric garbage
    # de-dupe but keep first occurrence order
    seen = set()
    ordered = []
    for r in out:
        if r not in seen:
            seen.add(r)
            ordered.append(r)
    return ordered

def _normalize_ref_series(s: pd.Series) -> pd.Series:
    """
    Make 'Ref' comparable against parsed strings:
    - drop decimals like 25.0 -> '25'
    - strip spaces
    - convert NaN to None-like empty string
    """
    def _to_ref_str(v):
        if pd.isna(v):
            return ""
        try:
            # if it looks like an int-ish float, cast to int
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
    """
    If `refs` is provided (e.g. '25-32, 41'), select those rows by Ref (in that order).
    If `refs` is None and not testing, prompt the user.
    Otherwise, fall back to using the tail of the sourcing sheet (`n_rows`).
    """
    sourcing_path = os.path.join(DATA_DIR, "Sourcing and quoting date_v4.xlsx")
    df = cached_read_excel(sourcing_path, cache_dir=CACHE_DIR, force_reload=force_reload)

    if refs is None and not testing:
        user_in = input("Enter Ref numbers/ranges (e.g. 25-32, 41) or press Enter for tail: ").strip()
    else:
        user_in = refs or ""

    selected_refs = _parse_ref_selector(user_in)

    if selected_refs and "Ref" in df.columns:
        ref_col = _normalize_ref_series(df["Ref"])
        # preserve the user-specified order
        order = pd.Categorical(ref_col, categories=selected_refs, ordered=True)
        picked = df.loc[ref_col.isin(selected_refs)].copy()
        picked["_order"] = pd.Categorical(_normalize_ref_series(picked["Ref"]), categories=selected_refs, ordered=True)
        picked = picked.sort_values("_order").drop(columns=["_order"])
        if picked.empty:
            print("[WARN] No rows matched the given Ref values; falling back to tail.")
            return df.tail(n_rows)
        return picked

    # fallback: tail
    return df.tail(n_rows)


