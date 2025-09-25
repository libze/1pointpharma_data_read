# run.py
import argparse
from reader import read_data, get_input_rows
from reader_logic import (
    sourcing, product_catalog, legemidler,
    check_availability_no, special_access, export_matches_to_excel
)

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
    mux = parser.add_mutually_exclusive_group()
    mux.add_argument("-n", "--rows", type=int, default=3,
                     help="Use the last N rows from sourcing (tail).")
    mux.add_argument("--refs", type=str, default=None,
                     help="Select input rows by Ref (e.g. '25-32,41,45-46').")

    parser.add_argument("-f", "--force-reload", action="store_true",
                        help="Force reload of all Excel files (ignore caches).")
    parser.add_argument("-o", "--output", default="match_results.xlsx",
                        help="Output Excel filename (written under Output/).")
    parser.add_argument("-s", "--sheet", default=None,
                        help="Optional sheet name to create in the output workbook.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print progress messages.")
    parser.add_argument("--add-spaces", action="store_true",
                        help="Insert blank separator rows between sources (off by default).")

    args = parser.parse_args(argv)

    if args.verbose:
        print("Loading data (cached)…")

    files_map = read_data(files=None, force_reload=args.force_reload, return_dict=True)

    sourcing_df   = _pick_df(files_map, REQUIRED_SOURCES["sourcing"])
    legemidler_df = _pick_df(files_map, REQUIRED_SOURCES["legemidler"])
    topp_df       = _pick_df(files_map, REQUIRED_SOURCES["topp"])
    special_df    = _pick_df(files_map, REQUIRED_SOURCES["special"])
    product_df    = _pick_df(files_map, REQUIRED_SOURCES["product"])

    rows = get_input_rows(
        testing=(args.refs is None),
        force_reload=args.force_reload,
        n_rows=args.rows,
        refs=args.refs
    )

    if args.verbose:
        if args.refs:
            print(f"Using {len(rows)} row(s) selected by Ref: {args.refs}")
        else:
            print(f"Using the last {len(rows)} input rows from sourcing.")

    sourcing_dict        = sourcing(rows, sourcing_df)
    product_catalog_dict = product_catalog(rows, product_df)
    legemidler_dict      = legemidler(rows, legemidler_df)
    topp_dict            = check_availability_no(rows, topp_df)
    special_dict         = special_access(rows, special_df)

    all_matches = {
        "product_catalog": product_catalog_dict,
        "legemidler":      legemidler_dict,
        "special_access":  special_dict,
        "sourcing":        sourcing_dict,
        "topp 500":        topp_dict,
    }

    if args.verbose:
        print("Exporting to Excel…")

    export_matches_to_excel(
        all_matches,
        rows,
        sheet_name=args.sheet,
        filename=args.output,
        add_spaces=args.add_spaces,
    )

    if args.verbose:
        print("Done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
