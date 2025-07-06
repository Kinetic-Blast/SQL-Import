import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
import logging
from io import StringIO
from datetime import datetime, timedelta
from pathlib import Path
import traceback

# Configure logging to capture print statements
log_stream = StringIO()
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(log_stream)]
)
logger = logging.getLogger()

#=========================================================================================
def send_email(log_messages, invalid_imports):
    if log_messages or invalid_imports:
        msg = MIMEMultipart("alternative")
        subject = 'File Import Report'
        msg['Subject'] = subject 
        msg['From'] = '<REDACTED_FROM_EMAIL>'
        msg['To'] = '<REDACTED_TO_EMAIL>'

        text_body = "Import process report:\n\n"
        html_body = "<html><body><h3>Import process report:</h3><pre style='font-family: monospace;'>"

        if log_messages:
            log_sections = log_messages.split('Processing import for ')
            for i, section in enumerate(log_sections):
                if not section:
                    continue
                section_text = (f"Processing import for {section}" if i > 0 else section).strip()
                separator = '-' * 80
                formatted = f"{separator}\n{section_text}\n{separator}\n\n"
                text_body += formatted
                html_body += formatted

        if invalid_imports:
            text_body += "Invalid Imports Detected:\n\n"
            html_body += "Invalid Imports Detected:\n\n"
            for import_group in invalid_imports:
                if import_group:
                    file_logs = '\n'.join(f"  - {name}" for name in import_group) + '\n\n'
                    text_body += "File import log:\n" + file_logs
                    html_body += "File import log:\n" + file_logs

        html_body += "</pre></body></html>"

        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        s = smtplib.SMTP('<SMTP_SERVER>')
        s.send_message(msg)
        s.quit()

#=========================================================================================
def get_sqlalchemy_engine(database):
    computer_name = os.environ.get('COMPUTERNAME', '')
    driver = 'ODBC Driver 17 for SQL Server' if computer_name in ['DEV-MACHINE1', 'DEV-MACHINE2'] else 'SQL Server'

    connection_url = URL.create(
        "mssql+pyodbc",
        username=None,
        password=None,
        host="<SQL_SERVER>",
        database=database,
        query={"driver": driver, "Trusted_Connection": "yes"}
    )

    return create_engine(connection_url, use_setinputsizes=False)

#=========================================================================================
def Get_file_data_import(file_path, delimiter):
    try:
        df = pd.read_csv(file_path, delimiter=delimiter)
        df = df.where(pd.notnull(df), None)
        return df
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return None

def get_table_columns_and_types(database, table_name):
    engine = get_sqlalchemy_engine(database)
    
    query = text(f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM [{database}].INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION;
    """)

    with engine.connect() as connection:
        result = connection.execute(query, {"table_name": table_name})
        rows = result.fetchall()

    return rows

#=========================================================================================
def adjust_dataframe_to_table(df, database, table_name):
    columns = get_table_columns_and_types(database, table_name)
    
    skipped_column = columns[0][0] if columns else None
    columns = columns[1:]
    
    column_names = [col[0] for col in columns]
    column_lengths = {}

    for col_name, data_type, max_length in columns:
        if data_type in ['varchar', 'nvarchar'] and max_length not in (None, -1):
            column_lengths[col_name] = int(max_length)

    df_columns = df.columns.tolist()
    extra_columns = [col for col in df_columns if col not in column_names]
    missing_columns = [col for col in column_names if col not in df_columns]

    if extra_columns:
        logger.warning(f"Dropping columns not in table schema: {extra_columns}")
    if missing_columns:
        logger.warning(f"Adding missing columns (filled with None): {missing_columns}")
    
    df = df[[col for col in df.columns if col in column_names]]

    for col in missing_columns:
        df[col] = None

    for col, max_len in column_lengths.items():
        if col in df.columns:
            df[col] = df[col].astype(str).str[:max_len]

    df = df[column_names]

    return df

#=========================================================================================
def import_file_to_sql(file_path, database, table_name, invalid_imports, delimiter):
    df = Get_file_data_import(file_path, delimiter)

    if df is None or df.empty:
        logger.error(f"No data to import for {file_path} in {database}.{table_name}")
        invalid_imports.append([f"No data to import: {file_path}"])
        return False, invalid_imports
    
    try:
        engine = get_sqlalchemy_engine(database)
        df = adjust_dataframe_to_table(df, database, table_name)
        df.to_sql(
            name=table_name,
            con=engine,
            schema="dbo",
            index=False,
            if_exists='append'
        )
        logger.info(f"Data imported successfully into {database}.{table_name} from {file_path}")
        return True, invalid_imports
    except Exception as e:
        logger.error(f"Error during import to {database}.{table_name} from {file_path}: {e}")
        traceback.print_exc()
        invalid_imports.append([f"Import failed for {file_path} in {database}.{table_name}: {str(e)}"])
        return False, invalid_imports

#=========================================================================================
def process_imports(import_configs):
    invalid_imports = []
    log_stream.seek(0)
    log_stream.truncate(0)
    
    for config in import_configs:
        if len(config) != 4:
            logger.error(f"Invalid configuration format: {config}")
            invalid_imports.append([f"Invalid configuration format: {config}"])
            continue
        
        file_path, delimiter, database, table_name = config
        info_delimiter = "TAB" if delimiter == "\t" else delimiter

        logger.info("Processing import for %s into %s.%s using delimiter '%s'", file_path, database, table_name, info_delimiter)
        success, invalid_imports = import_file_to_sql(file_path, database, table_name, invalid_imports, delimiter)
    
    log_messages = log_stream.getvalue()
    send_email(log_messages, invalid_imports)

    return invalid_imports

#=========================================================================================
def get_periodic_import_files(imports):
    current_time = datetime.now()
    cutoff_time = current_time - timedelta(hours=24)

    import_definitions = [
        [fr"<REDACTED_PATH>\{current_time.year}_{current_time.month:02}", "AccountSummary_*.txt", "\t", "ExampleDB", "Account_Summary_Table"],
        # Add more entries as needed...
    ]

    for base_path, pattern, delimiter, database, table in import_definitions:
        base = Path(base_path)
        if base.exists():
            for file in base.glob(pattern):
                if datetime.fromtimestamp(file.stat().st_mtime) >= cutoff_time:
                    imports.append([str(file), delimiter, database, table])

    return imports

#=========================================================================================

imports = []
imports = get_periodic_import_files(imports)
process_imports(imports)
