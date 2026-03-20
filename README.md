# Parquet2CSV

This project contains a Python script that recursively searches the `input` directory for `.parquet` files and writes them as `.csv` files to the `output` directory.

## How It Works

- searches `input` including all subdirectories for `.parquet` files
- creates `output` automatically if the directory does not exist yet
- converts every matching file into a CSV file
- preserves the directory structure from `input` inside `output`
- allows the CSV delimiter and decimal separator to be defined via command-line parameters

Example:

- `input\data\example.parquet` becomes `output\data\example.csv`

## Requirements

- Python 3.10 or newer
- installed dependencies from `requirements.txt`

## Installation

Recommended: use a virtual environment with `venv`.

Create a virtual environment:

```powershell
python -m venv .venv
```

Activate it on Windows in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Activate it on Windows in Command Prompt:

```cmd
.venv\Scripts\activate.bat
```

Activate it on Linux or macOS:

```bash
source .venv/bin/activate
```

Install the dependencies inside the virtual environment:

```powershell
pip install -r requirements.txt
```

## Usage

Recommended order when working with `venv`:

1. Create the virtual environment once with `python -m venv .venv`.
2. Activate the virtual environment.
3. Install the dependencies with `pip install -r requirements.txt`.
4. Run the script.
5. Deactivate the virtual environment after you are finished working.

On macOS, the `venv` workflow is usually the same as on Linux.

```powershell
python convert_parquet_to_csv.py
```

Show parameter help:

```powershell
python convert_parquet_to_csv.py --help
```

Example using a semicolon as the CSV delimiter and a comma as the decimal separator:

```powershell
python convert_parquet_to_csv.py --delimiter ";" --decimal ","
```

Deactivate the virtual environment when you are done:

```powershell
deactivate
```

## Parameters

- `--delimiter`: defines the delimiter used in the generated CSV files, default is `,`
- `--decimal`: defines the decimal separator used for numeric values, default is `.`

Notes:

- both parameters must be exactly one character long
- `--delimiter` and `--decimal` must not use the same value

## Project Structure

```text
Parquet2CSV/
|-- input/
|-- output/
|-- convert_parquet_to_csv.py
|-- requirements.txt
|-- README.md
```

## Notes

- If no `.parquet` files are found, the script prints a corresponding message.
- If `input` does not exist, the script exits with an error message.
- `pyarrow` is used to read the Parquet files.
- Help for all parameters is available through `-h` or `--help`.