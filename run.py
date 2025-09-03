# run.py
import argparse
import sys
from reader import read_data, get_input_rows
# import your functions from the file where they live:
from reader_logic import sourcing, product_catalog, legemidler, check_availability_no, special_access, export_matches_to_excel

# ---- import paths ----
# If your functions are in the same file you run, adjust the import above accordingly.

REQUIRED_SOURCES = {
    "sourcing": "sourcing and quoting",
    "legemidler": "legemiddel",
    "topp": "topp 500",
    "special": "special access list",
    "product": "product catalog",
}

def _pick_df(files_map, key_contains: str):
    key_contains = key_contains.lower()
    for name, df in files_map.items():
        if key_contains in name.lower():
            return df
    raise FileNotFoundError(f"Required file containing '{key_contains}' not found in Data/")

def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the meds matching pipeline end-to-end.")
    parser.add_argument("-n", "--rows", type=int, default=3, help="Number of input rows to use from the sourcing sheet (tail).")
    parser.add_argument("-f", "--force-reload", action="store_true", help="Force reload of all Excel files (ignore caches).")
    parser.add_argument("-o", "--output", default="match_results.xlsx", help="Output Excel filename (will be written under Output/).")
    parser.add_argument("-s", "--sheet", default=None, help="Optional sheet name to create in the output workbook.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print a bit more along the way.")
    args = parser.parse_args(argv)

    if args.verbose:
        print("Loading data (cached)…")

    # Load all Excel files in Data/ as a dict {basename: DataFrame}
    files_map = read_data(files=None, force_reload=args.force_reload, return_dict=True)

    # Pick the 5 sheets we need by fuzzy name
    try:
        sourcing_df   = _pick_df(files_map, REQUIRED_SOURCES["sourcing"])
        legemidler_df = _pick_df(files_map, REQUIRED_SOURCES["legemidler"])
        topp_df       = _pick_df(files_map, REQUIRED_SOURCES["topp"])
        special_df    = _pick_df(files_map, REQUIRED_SOURCES["special"])
        product_df    = _pick_df(files_map, REQUIRED_SOURCES["product"])
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        return 1

    # Input rows (tail N)
    rows = get_input_rows(testing=True, force_reload=args.force_reload, n_rows=args.rows)
    if args.verbose:
        print(f"Using the last {len(rows)} input rows from sourcing.")

    # Build match dicts
    sourcing_dict       = sourcing(rows, sourcing_df)
    product_catalog_dict= product_catalog(rows, product_df)
    legemidler_dict     = legemidler(rows, legemidler_df)
    topp_dict           = check_availability_no(rows, topp_df)
    special_dict        = special_access(rows, special_df)

    all_matches = {
        "product_catalog": product_catalog_dict,
        "legemidler":      legemidler_dict,
        "special_access":  special_dict,
        "sourcing":        sourcing_dict,
        "topp 500":        topp_dict,
    }

    if args.verbose:
        print("Exporting to Excel…")

    # Your existing exporter constructs Output/ and writes there
    export_matches_to_excel(all_matches, rows, sheet_name=args.sheet, filename=args.output)

    if args.verbose:
        print("Done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
