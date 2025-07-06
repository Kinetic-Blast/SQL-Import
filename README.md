
# File Import Automation System

This Python script automates the import of structured text files into a Microsoft SQL Server database. It is designed for use in operational environments where recurring data drops (e.g., monthly vendor reports) need to be ingested, validated, and logged.

## Features

- Monitors folders for newly modified files (within 24 hours).
- Supports multiple file patterns and import targets.
- Auto-adjusts DataFrame to match target SQL table schema:
  - Trims strings to column limits
  - Fills missing columns
  - Drops extra columns
- Uses SQLAlchemy for database interaction.
- Logs all operations and errors.
- Sends a summary email with plain text and HTML reports.

## Requirements

- Python 3.7+
- SQL Server with Windows Authentication
- Dependencies:
  - pandas
  - sqlalchemy
  - pyodbc
  - smtplib (standard)
  - email (standard)
  - pathlib (standard)

Install with:

```bash
pip install pandas sqlalchemy pyodbc


## File Structure

```
import_script.py      # Main import automation logic
README.md             # This documentation
```

## Configuration

Update the following hardcoded values in `import_script.py` to match your environment:

### Email Settings

```python
msg['From'] = '<REDACTED_FROM_EMAIL>'
msg['To'] = '<REDACTED_TO_EMAIL>'
s = smtplib.SMTP('<REDACTED_SMTP_SERVER>')
```

### SQL Server Connection

```python
host="<REDACTED_SQL_SERVER>",
driver = 'ODBC Driver 17 for SQL Server'
```

### Import Definitions

In the `get_periodic_import_files()` function, define folders and file patterns:

```python
import_definitions = [
    [r"E:\Your\Path\2025_07", "SomeFilePattern_*.txt", "\t", "YourDatabase", "YourTable"]
]
```

## Usage

To run the import:

```bash
python import_script.py
```

This will:

* Check file modification timestamps.
* Import data from matching files into the appropriate SQL table.
* Email a summary report to the configured team.

## Extending

To add a new import file type:

1. Add a new entry to `import_definitions` with:

   * Directory
   * Filename pattern
   * Delimiter
   * Target SQL database
   * Target table
2. Ensure the database table exists and matches expected file columns.
3. Test the script manually before scheduling.

## Logging

Logs are captured in-memory and sent via email. You can optionally direct them to a file by modifying the logging setup:

```python
logging.basicConfig(filename='import_log.txt', level=logging.INFO, ...)
```

## Caveats

* No file deduplication or state tracking (may re-import files).
* Not concurrency-safe (avoid running in parallel).
* Requires manual config for each import type.
* No retry logic for failed imports.
