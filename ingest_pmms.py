import requests
import csv
import sqlite3
import logging
from datetime import datetime
import os
import gzip
import shutil
from pathlib import Path

# Load configuration
def load_config():
    """Load configuration from config file."""
    config_file = Path(__file__).with_suffix('.config')
    
    # Default configuration
    config = {
        'DB_NAME': "pmms_data.db",
        'TABLE_NAME': "pmms_rates",
        'CSV_URL': "https://www.freddiemac.com/pmms/docs/PMMS_history.csv",
        'HTTP_PROXY': None,
        'HTTPS_PROXY': None,
        'PROXY_USERNAME': "",
        'PROXY_PASSWORD': "",
        'LOG_LEVEL': "INFO",
        'LOG_FILE': "pmms_ingest.log",
        'LOG_MAX_BYTES': 10 * 1024 * 1024,  # 10MB
        'LOG_BACKUP_COUNT': 3,
        'LOG_COMPRESS_BACKUPS': True,
        'REQUEST_TIMEOUT': 30,
        'PROGRESS_LOG_INTERVAL': 1000
    }
    
    # Load from file if exists
    if config_file.exists():
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Remove inline comments
                    if '#' in value:
                        value = value.split('#')[0].strip()
                    
                    # Handle different data types
                    if key in ['LOG_MAX_BYTES', 'REQUEST_TIMEOUT', 'PROGRESS_LOG_INTERVAL', 'LOG_BACKUP_COUNT']:
                        config[key] = int(value)
                    elif key == 'LOG_LEVEL':
                        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
                        level = value.strip('"\'').upper()
                        if level not in valid_levels:
                            raise ValueError(f"Invalid LOG_LEVEL '{value}'. Must be one of: {valid_levels}")
                        config[key] = level
                    elif key in ['LOG_COMPRESS_BACKUPS']:
                        config[key] = value.lower() in ('true', '1', 'yes', 'on')
                    elif key in ['HTTP_PROXY', 'HTTPS_PROXY']:
                        config[key] = value.strip('"\'') if value else None
                    elif key in ['PROXY_USERNAME', 'PROXY_PASSWORD']:
                        config[key] = value.strip('"\'') if value else ""
                    else:
                        config[key] = value.strip('"\'')
    
    return config

# Load configuration
CONFIG = load_config()

# Configuration variables
DB_NAME = CONFIG['DB_NAME']
TABLE_NAME = CONFIG['TABLE_NAME']
CSV_URL = CONFIG['CSV_URL']

# Setup proxy configuration
def get_proxies():
    """Setup proxy configuration for HTTP requests."""
    proxies = {}
    
    if CONFIG['HTTP_PROXY']:
        proxies['http'] = CONFIG['HTTP_PROXY']
        logger.info(f"Using HTTP proxy: {CONFIG['HTTP_PROXY']}")
    
    if CONFIG['HTTPS_PROXY']:
        proxies['https'] = CONFIG['HTTPS_PROXY']
        logger.info(f"Using HTTPS proxy: {CONFIG['HTTPS_PROXY']}")
    
    if not proxies:
        logger.info("No proxy configured - using direct connection")
    
    return proxies

# Setup proxy authentication if needed
def get_auth():
    """Setup proxy authentication if configured."""
    if CONFIG['PROXY_USERNAME'] and CONFIG['PROXY_PASSWORD']:
        return (CONFIG['PROXY_USERNAME'], CONFIG['PROXY_PASSWORD'])
    return None

# Configure logging with rotation
def setup_logging():
    """Setup logging with rotation and proper formatting."""
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, CONFIG['LOG_LEVEL']))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter with timestamp
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Rotating file handler with optional compression
    if CONFIG['LOG_COMPRESS_BACKUPS']:
        # Use custom handler for compressed backups
        from logging.handlers import RotatingFileHandler
        
        class CompressedRotatingFileHandler(RotatingFileHandler):
            def doRollover(self):
                """Do a rollover with compression of backup files."""
                if self.stream:
                    self.stream.close()
                    self.stream = None
                
                # Compress existing backup files
                for i in range(self.backupCount - 1, 0, -1):
                    src = f"{self.baseFilename}.{i}"
                    dst = f"{self.baseFilename}.{i + 1}"
                    
                    if os.path.exists(src):
                        if os.path.exists(dst):
                            os.remove(dst)
                        
                        # Compress the file
                        with open(src, 'rb') as f_in:
                            with gzip.open(f"{dst}.gz", 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        os.remove(src)
                
                # Move current file to .1
                if os.path.exists(self.baseFilename):
                    dst = f"{self.baseFilename}.1"
                    if os.path.exists(dst):
                        os.remove(dst)
                    self.rotate(self.baseFilename, dst)
                
                # Create new file
                if not self.delay:
                    self.stream = self._open()
        
        file_handler = CompressedRotatingFileHandler(
            CONFIG['LOG_FILE'],
            maxBytes=CONFIG['LOG_MAX_BYTES'],
            backupCount=CONFIG['LOG_BACKUP_COUNT'],
            encoding='utf-8'
        )
    else:
        # Standard rotating handler without compression
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            CONFIG['LOG_FILE'],
            maxBytes=CONFIG['LOG_MAX_BYTES'],
            backupCount=CONFIG['LOG_BACKUP_COUNT'],
            encoding='utf-8'
        )
    
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

def get_db_connection():
    """Creates and returns a connection to the SQLite database."""
    try:
        logger.info(f"Connecting to database: {DB_NAME}")
        conn = sqlite3.connect(DB_NAME)
        # Create table if it doesn't exist
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                date TEXT PRIMARY KEY,
                pmms30 REAL,
                pmms15 REAL
            )
        ''')
        logger.info(f"Database connection established, table '{TABLE_NAME}' ready")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise

def get_latest_date_from_db(conn):
    """Queries the DB to find the most recent date stored."""
    try:
        logger.info("Querying latest date from database")
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
        result = cursor.fetchone()
        latest_date = result[0] if result[0] else None
        logger.info(f"Latest date in database: {latest_date}")
        return latest_date
    except sqlite3.Error as e:
        logger.error(f"Failed to query latest date from database: {e}")
        raise

def parse_row(row):
    """
    Parses a CSV row dictionary into a clean format.
    Adjust column keys based on the actual CSV header names.
    """
    try:
        # Normalize keys to lowercase to handle header variations
        row = {k.strip().lower(): v for k, v in row.items()}
        
        # Check if date field exists
        if 'date' not in row:
            logger.warning(f"Row missing 'date' field: {row}")
            return None
        
        date_value = row['date'].strip()
        if not date_value:
            logger.warning(f"Empty date value in row: {row}")
            return None
        
        try:
            # Freddie Mac CSV dates are usually M/D/YYYY
            dt_obj = datetime.strptime(date_value, "%Y-%m-%d")
        except ValueError:
            try:
                dt_obj = datetime.strptime(date_value, "%m/%d/%Y")
            except ValueError as e:
                logger.warning(f"Failed to parse date '{date_value}': {e}")
                return None

        # Format date as YYYY-MM-DD for SQLite sorting
        formatted_date = dt_obj.strftime("%Y-%m-%d")
        
        # Extract rate values with fallbacks
        pmms30 = row.get('pmms30', None) or row.get('30-yr frm', None)
        pmms15 = row.get('pmms15', None) or row.get('15-yr frm', None)
        
        # Validate rate values
        if pmms30 is not None:
            try:
                pmms30 = float(pmms30)
            except (ValueError, TypeError):
                logger.warning(f"Invalid pmms30 value '{pmms30}' in row: {row}")
                pmms30 = None
                
        if pmms15 is not None:
            try:
                pmms15 = float(pmms15)
            except (ValueError, TypeError):
                logger.warning(f"Invalid pmms15 value '{pmms15}' in row: {row}")
                pmms15 = None
        
        return {
            "date": formatted_date,
            "pmms30": pmms30,
            "pmms15": pmms15
        }
    except Exception as e:
        logger.error(f"Unexpected error parsing row: {e}, row data: {row}")
        return None


def stream_and_ingest():
    logger.info("Starting streaming ingestion process")
    
    try:
        conn = get_db_connection()
        last_db_date = get_latest_date_from_db(conn)
        logger.info(f"Latest date in DB: {last_db_date if last_db_date else 'None (Empty DB)'}")

        new_records = []
        processed_rows = 0
        skipped_rows = 0
        
        # 1. STREAM: Open connection with stream=True
        logger.info(f"Initiating HTTP request to: {CSV_URL}")
        proxies = get_proxies()
        auth = get_auth()
        
        try:
            with requests.get(CSV_URL, stream=True, timeout=CONFIG['REQUEST_TIMEOUT'], 
                          proxies=proxies, auth=auth) as r:
                r.raise_for_status()
                logger.info(f"HTTP request successful, status code: {r.status_code}")
                logger.info(f"Content type: {r.headers.get('content-type', 'Unknown')}")
                logger.info(f"Content length: {r.headers.get('content-length', 'Unknown')}")
                
                # Create a generator that decodes lines on the fly
                lines = (line.decode('utf-8') for line in r.iter_lines())
                
                try:
                    # Use csv.DictReader on the generator
                    reader = csv.DictReader(lines)
                    logger.info(f"CSV headers detected: {reader.fieldnames}")
                    
                    for row in reader:
                        processed_rows += 1
                        clean_data = parse_row(row)
                        
                        if not clean_data:
                            skipped_rows += 1
                            continue

                        # 2. FILTER: Only keep records newer than what we have
                        if last_db_date is None or clean_data['date'] > last_db_date:
                            new_records.append(clean_data)
                            
                        # Log progress every N rows
                        if processed_rows % CONFIG['PROGRESS_LOG_INTERVAL'] == 0:
                            logger.info(f"Processed {processed_rows} rows, found {len(new_records)} new records, skipped {skipped_rows} rows")
                            
                except csv.Error as e:
                    logger.error(f"CSV parsing error: {e}")
                    raise
                except UnicodeDecodeError as e:
                    logger.error(f"Failed to decode CSV data: {e}")
                    raise
                    
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out after {CONFIG['REQUEST_TIMEOUT']} seconds")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Network connection error: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

        logger.info(f"Streaming completed. Processed {processed_rows} total rows, skipped {skipped_rows} invalid rows, found {len(new_records)} new records")

        # 3. LOAD: Insert only new records
        if new_records:
            logger.info(f"Inserting {len(new_records)} new records into database")
            
            try:
                # Update SQLite
                cursor = conn.cursor()
                for record in new_records:
                    cursor.execute(f'''
                        INSERT OR IGNORE INTO {TABLE_NAME} (date, pmms30, pmms15)
                        VALUES (?, ?, ?)
                    ''', (record['date'], record['pmms30'], record['pmms15']))
                conn.commit()
                logger.info("Successfully committed new records to SQLite DB")
            except sqlite3.Error as e:
                logger.error(f"Database insert failed: {e}")
                conn.rollback()
                raise
        else:
            logger.info("No new data found. Local storage is up to date")

    except Exception as e:
        logger.error(f"Fatal error in streaming ingestion: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    stream_and_ingest()