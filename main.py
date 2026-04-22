"""
main.py - CLI entry point for LGD Fuzzy Matcher
Usage:
    python main.py --input data.csv
    python main.py --input data.xlsx --output results.csv --sql updates.sql
    python main.py --input data.csv --use-mysql
"""
import argparse, os, sys, time
import pandas as pd
from matcher import LGDMatcher
from utils import setup_logging, load_config, save_matched_csv, save_unmatched_csv, generate_sql_update


def load_input(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":             return pd.read_csv(path, dtype=str)
    if ext in {".xlsx", ".xls"}: return pd.read_excel(path, dtype=str)
    raise ValueError(f"Unsupported format: {ext}. Use .csv or .xlsx")


def parse_args():
    p = argparse.ArgumentParser(description="LGD Fuzzy Matcher CLI")
    p.add_argument("--input",        required=True,  help="Input CSV/Excel file")
    p.add_argument("--output",       default="lgd_matched_output.csv")
    p.add_argument("--unmatched",    default="lgd_unmatched.csv")
    p.add_argument("--sql",          default="lgd_updates.sql")
    p.add_argument("--table",        default="target_table")
    p.add_argument("--config",       default="config.json")
    p.add_argument("--state-csv",    default="lgd_STATE.csv")
    p.add_argument("--district-csv", default="DISTRICT_STATE.csv")
    p.add_argument("--use-mysql",    action="store_true")
    return p.parse_args()


def main():
    args   = parse_args()
    cfg    = load_config(args.config)
    logger = setup_logging(cfg.get("logging", {}).get("log_file", "lgd_matcher.log"),
                           cfg.get("logging", {}).get("level", "INFO"))
    logger.info("LGD Matcher started | input=%s", args.input)

    try:
        input_df = load_input(args.input)
    except Exception as e:
        logger.error("Failed to load input: %s", e); sys.exit(1)

    missing = {"state_name_raw", "district_name_raw"} - set(input_df.columns)
    if missing:
        logger.error("Missing required columns: %s", missing); sys.exit(1)

    matcher = LGDMatcher(config_path=args.config)
    if args.use_mysql:
        matcher.load_master_from_mysql()
    else:
        matcher.load_master_from_csv(args.state_csv, args.district_csv)

    t0      = time.perf_counter()
    results = matcher.match_dataframe(input_df)
    elapsed = time.perf_counter() - t0

    save_matched_csv(results, args.output)
    save_unmatched_csv(results, args.unmatched)
    generate_sql_update(results, table_name=args.table, output_path=args.sql)

    summary = results["match_status"].value_counts().to_dict()
    print("\n" + "="*50)
    print("  LGD Matching Summary")
    print("="*50)
    print(f"  Total rows  : {len(results)}")
    print(f"  Time taken  : {elapsed:.2f}s  ({len(results)/max(elapsed,0.001):.0f} rows/sec)")
    for k, v in summary.items():
        print(f"  {k:<22}: {v}")
    print(f"\n  Output CSV  : {args.output}")
    print(f"  Unmatched   : {args.unmatched}")
    print(f"  SQL script  : {args.sql}")
    print("="*50)

if __name__ == "__main__":
    main()
