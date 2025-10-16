from dataclasses import dataclass
from enum import Enum, auto
from ftfcu_appworx import Apwx, JobTime
from oracledb import Connection as DbConnection
from pathlib import Path
from typing import Any, Optional, List, Dict
import csv
import datetime
import re
import unicodedata
import yaml

__version__ = "0.01"


class AppWorxEnum(Enum):
    """Define AppWorx arguments here to avoid hard-coded strings."""

    TNS_SERVICE_NAME = auto()
    FULL_CLEAN_YN = auto()
    DAYS_BACK = auto()
    RPT_ONLY_YN = auto()
    OUTPUT_FILE_PATH = auto()
    OUTPUT_FILE_NAME = auto()

    def __str__(self):
        return self.name


@dataclass
class ScriptData:
    """Class that holds all the structures and data needed by the script."""
    
    apwx: Apwx
    dbh: DbConnection
    config: Any
    data_ext: List[Dict] = None
    data_int: List[Dict] = None
    output_file: str = None


def run(apwx: Apwx) -> bool:
    """Main processing function for the script."""
    
    script_data = initialize(apwx)
    
    log("Starting RTXN Description Cleaner Job")
    
    log("Connecting to DNA DB")
    
    if script_data.apwx.args.FULL_CLEAN_YN == 'Y':
        log("Cleansing Fields - Full Clean")
    else:
        log("Cleansing Fields - Partial Clean")
    
    log("Getting Ext and Int Descriptions To Be Cleaned")
    
    # Get data based on full clean or incremental
    if script_data.apwx.args.FULL_CLEAN_YN == 'Y':
        script_data.data_ext = get_data_ext(script_data.dbh, script_data.config, full_clean=True)
        script_data.data_int = get_data_int(script_data.dbh, script_data.config, full_clean=True)
    else:
        script_data.data_ext = get_data_ext(script_data.dbh, script_data.config, full_clean=False, days_back=script_data.apwx.args.DAYS_BACK)
        script_data.data_int = get_data_int(script_data.dbh, script_data.config, full_clean=False, days_back=script_data.apwx.args.DAYS_BACK)
    
    log("Cleaning descriptions")
    clean_data(script_data)
    
    log("Updating database")
    update_desc_ext(script_data)
    update_desc_int(script_data)
    
    log("Writing Audit Report")
    write_report(script_data)
    
    log("Job Finished")
    
    return True


def get_data_ext(dbh: DbConnection, config: dict, full_clean: bool = True, days_back: int = 3) -> List[Dict]:
    """Get external transaction descriptions that need cleaning."""
    
    if full_clean:
        sql = config['queries']['get_ext_full']
    else:
        sql = config['queries']['get_ext_incremental']
        
    sql_params = {'days_back': days_back} if not full_clean else None
    
    return execute_sql(dbh, sql, 'SELECT', sql_params)


def get_data_int(dbh: DbConnection, config: dict, full_clean: bool = True, days_back: int = 3) -> List[Dict]:
    """Get internal transaction descriptions that need cleaning."""
    
    if full_clean:
        sql = config['queries']['get_int_full']
    else:
        sql = config['queries']['get_int_incremental']
        
    sql_params = {'days_back': days_back} if not full_clean else None
    
    return execute_sql(dbh, sql, 'SELECT', sql_params)


def clean_data(script_data: ScriptData) -> None:
    """Clean bad characters from description text."""
    
    # Clean external descriptions
    for record in script_data.data_ext:
        record['EXTRTXNDESCTEXT_OLD'] = record['EXTRTXNDESCTEXT']
        record['EXTRTXNDESCTEXT_NEW'] = clean_bad_chars(unidecode_text(record['EXTRTXNDESCTEXT'].strip()))
    
    # Clean internal descriptions
    for record in script_data.data_int:
        record['INTRTXNDESCTEXT_OLD'] = record['INTRTXNDESCTEXT']
        record['INTRTXNDESCTEXT_NEW'] = clean_bad_chars(unidecode_text(record['INTRTXNDESCTEXT'].strip()))


def clean_bad_chars(text: str) -> str:
    """Remove non-ASCII and non-printable characters."""
    # Remove non-ASCII characters
    text = ''.join(char for char in text if ord(char) < 128)
    # Remove non-printable characters
    text = ''.join(char for char in text if char.isprintable() or char.isspace())
    return text


def unidecode_text(text: str) -> str:
    """Convert Unicode text to ASCII equivalent."""
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')


def update_desc_ext(script_data: ScriptData) -> None:
    """Update external transaction descriptions in database."""
    
    sql = script_data.config['queries']['update_ext']
    
    # Prepare data for batch update
    update_data = []
    for record in script_data.data_ext:
        update_data.append({
            'new_text': record['EXTRTXNDESCTEXT_NEW'],
            'desc_nbr': record['EXTRTXNDESCNBR']
        })
    
    post_trans(script_data, sql, update_data)


def update_desc_int(script_data: ScriptData) -> None:
    """Update internal transaction descriptions in database."""
    
    sql = script_data.config['queries']['update_int']
    
    # Prepare data for batch update
    update_data = []
    for record in script_data.data_int:
        update_data.append({
            'new_text': record['INTRTXNDESCTEXT_NEW'],
            'desc_nbr': record['INTRTXNDESCNBR']
        })
    
    post_trans(script_data, sql, update_data)


def post_trans(script_data: ScriptData, sql: str, data: List[Dict]) -> None:
    """Execute batch transaction with commit/rollback logic."""
    
    try:
        with script_data.dbh.cursor() as cursor:
            batch_size = 5000
            total_records = len(data)
            
            for i in range(0, total_records, batch_size):
                batch = data[i:i + batch_size]
                cursor.executemany(sql, batch, batcherrors=True)
                
                # Check for batch errors
                batch_errors = cursor.getbatcherrors()
                if batch_errors:
                    report_errors("Posting errors: ", batch_errors)
                
                commit_rollback(script_data)
                log(f"Updated {min(i + batch_size, total_records)} records")
                
    except Exception as e:
        log(f"Error in post_trans: {e}")
        raise


def report_errors(msg: str, errors: List) -> None:
    """Report any batch processing errors."""
    if errors:
        for error in errors:
            log(f"{msg} Error: {error.message}")


def write_report(script_data: ScriptData) -> None:
    """Generate audit report in CSV format."""
    
    output_file = script_data.output_file
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        f.write('RTXN DESCRIPTION CLEANER AUDIT REPORT\n')
        f.write(f'RUN DATE: {datetime.datetime.now()}\n')
        f.write(f'REPORT ONLY YN: {script_data.apwx.args.RPT_ONLY_YN}\n\n')
        
        f.write('EXT DESCRIPTIONS\n')
        if script_data.data_ext:
            print_csv(f, script_data.data_ext, ['EXTRTXNDESCNBR', 'EXTRTXNDESCTEXT_OLD', 'EXTRTXNDESCTEXT_NEW'])
        
        f.write('\n\nINT DESCRIPTIONS\n')
        if script_data.data_int:
            print_csv(f, script_data.data_int, ['INTRTXNDESCNBR', 'INTRTXNDESCTEXT_OLD', 'INTRTXNDESCTEXT_NEW'])
        
        f.write('\nEND\n')


def print_csv(file_handle, data: List[Dict], field_order: List[str]) -> None:
    """Print data as CSV to file handle."""
    if not data:
        return
        
    writer = csv.DictWriter(file_handle, fieldnames=field_order, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    
    for record in data:
        # Only write fields that are in field_order
        filtered_record = {field: record.get(field, '') for field in field_order}
        writer.writerow(filtered_record)


def commit_rollback(script_data: ScriptData) -> None:
    """Commit or rollback based on RPT_ONLY_YN parameter."""
    if script_data.apwx.args.RPT_ONLY_YN == 'Y':
        script_data.dbh.rollback()
    else:
        script_data.dbh.commit()


def get_apwx() -> Apwx:
    """Creates the appworx object."""
    return Apwx(["OSIUPDATE", "OSIUPDATE_PW"])


def parse_args(apwx: Apwx) -> Apwx:
    """Validates the parameters provided to the script."""
    parser = apwx.parser
    parser.add_arg(str(AppWorxEnum.TNS_SERVICE_NAME), type=str, required=True)
    parser.add_arg(
        str(AppWorxEnum.FULL_CLEAN_YN), choices=["Y", "N"], default="N", required=False
    )
    parser.add_arg(
        str(AppWorxEnum.DAYS_BACK), type=int, default=3, required=False
    )
    parser.add_arg(
        str(AppWorxEnum.RPT_ONLY_YN), choices=["Y", "N"], default="Y", required=False
    )
    parser.add_arg(
        str(AppWorxEnum.OUTPUT_FILE_PATH), type=parser.dir_validator, default="./Logs", required=False
    )
    parser.add_arg(
        str(AppWorxEnum.OUTPUT_FILE_NAME), type=str, default="rtxn_desc_cleaner_audit_log.csv", required=False
    )
    apwx.parse_args()
    return apwx


def dna_db_connect(apwx) -> DbConnection:
    """Creates the database connection object."""
    return apwx.db_connect(autocommit=False)


def get_config() -> dict:
    """Loads config YAML into a dictionary."""
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def initialize(apwx) -> ScriptData:
    """Initialize objects required by the script to call external systems."""
    dbh = dna_db_connect(apwx)
    config = get_config()
    
    # Create output file path
    output_path = Path(apwx.args.OUTPUT_FILE_PATH)
    output_file = output_path / apwx.args.OUTPUT_FILE_NAME
    
    return ScriptData(apwx=apwx, dbh=dbh, config=config, output_file=str(output_file))


def execute_sql(conn: DbConnection, sql_statement: str, action: str, sql_params=None) -> List[Dict]:
    """Executes provided sql_statement with provided input parameters if present.
    Args:
        conn: Database connection object used to connect to DNA.
        sql_statement: The SQL statement to be executed.
        action: The specific type of SQL statement. This is used to determine how to execute.
        sql_params: Any bind variables that are used. Typically, this is a dictionary.
    Returns:
        SELECT statements will always return a list of dictionaries.
    """
    try:
        with conn.cursor() as cursor:
            if action == 'SELECT':
                if sql_params is not None:
                    cursor.execute(sql_statement, sql_params)
                else:
                    cursor.execute(sql_statement)
                        
                column_names = [col[0] for col in cursor.description]
                cursor.rowfactory = lambda *args: dict(zip(column_names, args))
                return cursor.fetchall()
                
            elif action in ('INSERT', 'UPDATE', 'MERGE', 'DELETE'):
                cursor.executemany(sql_statement, sql_params, batcherrors=True)
                batch_errors = cursor.getbatcherrors()
                if batch_errors:
                    for error in batch_errors:
                        error_index = error.offset
                        log(f'Error: {error.message} during {action}. Attempted input: {sql_params[error_index]}')
                    raise Exception(f'One or more errors occurred during the {action} process.')
                        
    except Exception as e:
        raise Exception(f"SQL error = {e}")


def log(message: str) -> None:
    """Log message with timestamp."""
    print(f"{datetime.datetime.now()}: {message}")


if __name__ == "__main__":
    JobTime().print_start()
    run(parse_args(get_apwx()))
    JobTime().print_end()