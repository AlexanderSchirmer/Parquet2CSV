# Parquet2CSV

A simple script to convert `.parquet` files to `.csv` files. The script automatically reads all Parquet files from the `input` directory and saves the converted CSV files in the `output` directory.

## Prerequisites & Installation

You need **Python 3.10 or newer**. It is recommended to run the script in a virtual environment (`venv`) to avoid conflicts with other Python packages.

**1. Create a virtual environment:**

```powershell
python -m venv .venv
```

**2. Activate the virtual environment:**

On Windows in PowerShell:
```powershell
.\.venv\Scripts\Activate.ps1
```
*(Alternative: `cmd`: `.venv\Scripts\activate.bat` | Linux/macOS: `source .venv/bin/activate`)*

**3. Install dependencies:**

```powershell
pip install -r requirements.txt
```

## Folder Structure

Make sure your files and directories are organized like this before running the script:

```text
your_folder/
|-- input/                      <-- Put your .parquet files here
|-- convert_parquet_to_csv.py   <-- The script
|-- requirements.txt            <-- Dependencies
```
*(The `output/` folder will be created automatically when you run the script).*

## Usage

1. Copy all `.parquet` files you want to convert into the `input/` folder. (Subfolders are allowed and will be preserved).
2. Make sure your virtual environment is activated.
3. Start the conversion:

```powershell
python convert_parquet_to_csv.py
```

The converted `.csv` files will automatically be saved in the `output/` folder.
When you are finished, you can leave the virtual environment by running the `deactivate` command.

### Advanced Settings (Parameters)

You can customize the format of the exported CSV file, which is especially useful for some spreadsheet software (like German Excel):

- `--delimiter`: Sets the delimiter (default is a comma `,`)
- `--decimal`: Sets the decimal separator (default is a period `.`)

**Example for German CSV format (delimiter `;` and comma as decimal separator):**
```powershell
python convert_parquet_to_csv.py --delimiter ";" --decimal ","
```

You can view all available commands with `python convert_parquet_to_csv.py --help`.

## Rules & Behavior

- Both parameters (`--delimiter` and `--decimal`) must be exactly one character long and must not use the same value.
- If no `.parquet` files are found, the script prints a corresponding message.
- If `input` does not exist, the script exits with an error message.
- The console shows the current file, the progress within that file, and the total progress across all files.