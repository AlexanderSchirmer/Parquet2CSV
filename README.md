# Parquet2CSV

This repository contains a Python script that recursively searches the `input` directory for `.parquet` files and writes them as `.csv` files to the `output` directory.

## What It Does

- searches `input` including all subdirectories for `.parquet` files
- creates `output` automatically if it does not exist yet
- converts every matching file into a CSV file
- preserves the directory structure from `input` inside `output`
- supports custom CSV delimiters and decimal separators
- shows progress in the console while files are being converted

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

## Project Structure

```text
Parquet2CSV/
|-- input/
|-- output/
|-- convert_parquet_to_csv.py
|-- requirements.txt
|-- README.md
```

Place the `.parquet` files you want to convert into `input`.

## Usage

After the virtual environment is activated and the dependencies are installed, run the script with:

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

When you are finished, you can deactivate the virtual environment:

```powershell
deactivate
```

## Parameters

- `--delimiter`: defines the delimiter used in the generated CSV files, default is `,`
- `--decimal`: defines the decimal separator used for numeric values, default is `.`

Rules:

- both parameters must be exactly one character long
- `--delimiter` and `--decimal` must not use the same value

## Behavior

- If no `.parquet` files are found, the script prints a corresponding message.
- If `input` does not exist, the script exits with an error message.
- Converted files are written to `output` with the same relative folder structure as in `input`.
- The console shows the current file, the progress within that file, and the total progress across all files.