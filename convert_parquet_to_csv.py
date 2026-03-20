from __future__ import annotations

import argparse
import csv
import sys
from decimal import Decimal
from importlib import import_module
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
DEFAULT_DELIMITER = ","
DEFAULT_DECIMAL_SEPARATOR = "."


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Searches the 'input' directory for Parquet files and converts them "
            "to CSV files in the 'output' directory."
        )
    )
    parser.add_argument(
        "--delimiter",
        default=DEFAULT_DELIMITER,
        help="CSV delimiter, for example ',' or ';' (default: ',').",
    )
    parser.add_argument(
        "--decimal",
        default=DEFAULT_DECIMAL_SEPARATOR,
        help="Decimal separator for numeric values, for example '.' or ',' (default: '.').",
    )

    args = parser.parse_args()

    if len(args.delimiter) != 1:
        parser.error("--delimiter must be exactly one character long.")

    if len(args.decimal) != 1:
        parser.error("--decimal must be exactly one character long.")

    if args.delimiter == args.decimal:
        parser.error("--delimiter and --decimal must not be identical.")

    return args


def format_value(value: object, decimal_separator: str) -> str:
    if value is None:
        return ""

    if isinstance(value, (float, Decimal)):
        return str(value).replace(".", decimal_separator)

    return str(value)


def write_csv(table: object, csv_path: Path, delimiter: str, decimal_separator: str) -> None:
    with csv_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file, delimiter=delimiter)
        writer.writerow(table.column_names)

        for row in table.to_pylist():
            writer.writerow(
                [format_value(row.get(column_name), decimal_separator) for column_name in table.column_names]
            )


def load_parquet_module() -> object:
    try:
        return import_module("pyarrow.parquet")
    except ImportError:
        print(
            "Missing dependency: pyarrow. Install it with 'pip install pyarrow'.",
            file=sys.stderr,
        )
        raise SystemExit(1)


def convert_parquet_files(
    input_dir: Path,
    output_dir: Path,
    delimiter: str,
    decimal_separator: str,
) -> int:
    pq = load_parquet_module()
    parquet_files = sorted(input_dir.rglob("*.parquet"))

    if not parquet_files:
        print(f"No Parquet files found in '{input_dir}'.")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)

    converted_count = 0
    for parquet_file in parquet_files:
        relative_path = parquet_file.relative_to(input_dir)
        csv_path = output_dir / relative_path.with_suffix(".csv")
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        table = pq.read_table(parquet_file)
        write_csv(table, csv_path, delimiter, decimal_separator)

        converted_count += 1
        print(f"Converted: '{parquet_file}' -> '{csv_path}'")

    return converted_count


def main() -> int:
    args = parse_args()

    if not INPUT_DIR.exists():
        print(f"Input directory '{INPUT_DIR}' does not exist.", file=sys.stderr)
        return 1

    converted_count = convert_parquet_files(
        INPUT_DIR,
        OUTPUT_DIR,
        args.delimiter,
        args.decimal,
    )
    print(f"Done. {converted_count} file(s) converted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())