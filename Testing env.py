import os
import glob
import json
import pickle
import pandas as pd

DATA_DIR  = "Data"
CACHE_DIR = "Cache"

# ---------- caching helpers (unchanged) ----------
def _file_fingerprint(path: str) -> dict | None:
    try:
        st = os.stat(path)
        return {"size": st.st_size, "mtime": st.st_mtime}
    except FileNotFoundError:
        return None

def _meta_path(cache_path: str) -> str:
    return cache_path + ".meta.json"

def _load_cache_meta(cache_path: str) -> dict | None:
    try:
        with open(_meta_path(cache_path), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _write_cache(cache_path: str, df: pd.DataFrame, fingerprint: dict) -> None:
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "wb") as f:
        pickle.dump(df, f)
    with open(_meta_path(cache_path), "w", encoding="utf-8") as f:
        json.dump({"fingerprint": fingerprint}, f)

def cached_read_excel(file_path: str, cache_dir: str = CACHE_DIR, force_reload: bool = False, **kwargs) -> pd.DataFrame:
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, os.path.basename(file_path) + ".pkl")

    fp = _file_fingerprint(file_path)
    if fp is None:
        raise FileNotFoundError(f"Missing data file: {file_path}")

    if not force_reload and os.path.exists(cache_path):
        meta = _load_cache_meta(cache_path)
        if meta and meta.get("fingerprint") == fp:
            with open(cache_path, "rb") as f:
                return pickle.load(f)

    read_kwargs = dict(kwargs)
    if "legemiddel" in os.path.basename(file_path).lower() and "skiprows" not in read_kwargs:
        read_kwargs["skiprows"] = 2

    df = pd.read_excel(file_path, **read_kwargs)
    _write_cache(cache_path, df, fp)
    return df

def _expand_files(files) -> list[str]:
    if isinstance(files, str):
        files = [files]
    paths: list[str] = []
    for pattern in files:
        base = pattern if os.path.sep in pattern else os.path.join(DATA_DIR, pattern)
        paths.extend(glob.glob(base))
    return sorted(set(paths))

# ---------- new zero-arg read_data ----------
def read_data(files: str | list[str] | None = None,
              force_reload: bool = False,
              concat_legemiddel: bool = True) -> list[pd.DataFrame]:
    """
    If `files` is None, auto-load the standard sources from Data/:
      0) Sourcing and quoting date_v4.xlsx
      1) legemiddelpriser-*.xlsx (concatenated unless concat_legemiddel=False -> latest file only)
      2) Topp 500 - Apotek 1.xlsx
      3) Special access list.xlsx
      4) Product catalog_v3.xlsx

    Returns a list in that exact order.
    If `files` is a filename/glob (old behavior), returns a list of DataFrames for those files.
    """
    if files is not None:
        # legacy behavior
        paths = _expand_files(files)
        return [cached_read_excel(p, force_reload=force_reload) for p in paths]

    # Auto mode
    sourcing_paths = _expand_files("Sourcing and quoting date_v4.xlsx")
    legem_paths    = _expand_files("legemiddelpriser-*.xlsx")
    topp_paths     = _expand_files("Topp 500 - Apotek 1.xlsx")
    special_paths  = _expand_files("Special access list.xlsx")
    product_paths  = _expand_files("Product catalog_v3.xlsx")

    if not sourcing_paths:
        raise FileNotFoundError("Cannot find 'Sourcing and quoting date_v4.xlsx' in Data/")
    if not legem_paths:
        raise FileNotFoundError("Cannot find any 'legemiddelpriser-*.xlsx' in Data/")
    if not topp_paths:
        raise FileNotFoundError("Cannot find 'Topp 500 - Apotek 1.xlsx' in Data/")
    if not special_paths:
        raise FileNotFoundError("Cannot find 'Special access list.xlsx' in Data/")
    if not product_paths:
        raise FileNotFoundError("Cannot find 'Product catalog_v3.xlsx' in Data/")

    sourcing_df = cached_read_excel(sourcing_paths[0], force_reload=force_reload)

    if concat_legemiddel:
        legemidler_df = pd.concat(
            [cached_read_excel(p, force_reload=force_reload) for p in legem_paths],
            ignore_index=True
        )
    else:
        latest = max(legem_paths, key=lambda p: os.stat(p).st_mtime)
        legemidler_df = cached_read_excel(latest, force_reload=force_reload)

    topp_df    = cached_read_excel(topp_paths[0],    force_reload=force_reload)
    special_df = cached_read_excel(special_paths[0], force_reload=force_reload)
    product_df = cached_read_excel(product_paths[0], force_reload=force_reload)

    return [sourcing_df, legemidler_df, topp_df, special_df, product_df]

def get_input_rows(testing: bool = True, force_reload: bool = False) -> pd.DataFrame:
    """
    Reads the sourcing file via the same cache logic and returns the last N rows.
    """
    if not testing:
        input_rows = int(input("How many rows should be taken into account?\n"))
    else:
        input_rows = 3
    sourcing_path = os.path.join(DATA_DIR, "Sourcing and quoting date_v4.xlsx")
    df = cached_read_excel(sourcing_path, force_reload=force_reload)
    return df.tail(input_rows)

read_data()