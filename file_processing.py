"""
File Processing System
Recursively processes files in directories, calculates checksums and stores metadata in database.
"""
import os
import hashlib
import logging
from typing import Iterator, Tuple, Optional
from configparser import ConfigParser
import sqlite3

import os
import sys
import logging
from typing import Any, Dict, Optional
import sqlite3
import configparser

def load_configuration(config_path: Optional[str] = None) -> configparser.ConfigParser:
    """Load configuration with safe defaults following policy requirements"""
    config = configparser.ConfigParser()
    
    # Determine config path
    final_path = config_path if config_path else (
        sys.argv[1] if len(sys.argv) > 1 else 'config.cfg'
    )
    
    if not os.path.exists(final_path):
        raise FileNotFoundError(f"Config file not found: {final_path}")
    
    config.read(final_path)
    
    # Validate minimum required sections
    sections = config.sections() 
    required_sections = {'LOGGING', 'DATABASE'}
    missing = required_sections - set(config.sections())
    if missing:
        raise ValueError(f"Missing required config sections: {missing}")
    
    return config

def configure_logging(config: configparser.ConfigParser) -> None:
    """Initialize standardized logging following policy requirements"""
    log_dir = os.path.dirname(config['LOGGING']['file_path'])
    os.makedirs(log_dir, exist_ok=True)
    
    log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # File handler
    logging.basicConfig(
        level=config['LOGGING'].get('level', 'INFO'),
        format=log_format,
        datefmt=date_format,
        filename=config['LOGGING']['file_path'],
        filemode='a'
    )
    
    # Add console handler if debug mode
    if config['LOGGING'].getboolean('debug', False):
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.setFormatter(logging.Formatter(log_format, datefmt=date_format))
        logging.getLogger().addHandler(console)

class DatabaseManager:
    """Safe database operations wrapper following policy requirements"""
    def __init__(self, config: configparser.ConfigParser):
        self.db_path = config['DATABASE']['path']
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        return self.conn.cursor()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()

def log_error(context: str, error: Exception) -> None:
    """Standardized error reporting following policy requirements"""
    logger = logging.getLogger(__name__)
    logger.error(
        f"{context} - {type(error).__name__}: {str(error)}",
        exc_info=True
    )

def calculate_md5(file_path: str, block_size: int = 65536) -> str:
    """Calculate MD5 hash of a file in chunks to handle large files."""
    md5 = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            for block in iter(lambda: f.read(block_size), b''):
                md5.update(block)
        return md5.hexdigest()
    except Exception as e:
        log_error(f"Failed to calculate MD5 for {file_path}", e)
        raise

def get_file_info(file_path: str) -> Tuple[str, int, str]:
    """Get file name, size and MD5 hash."""
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    file_hash = calculate_md5(file_path)
    return (file_name, file_size, file_hash)

def walk_directory(root_path: str) -> Iterator[str]:
    """Recursively walk through directory and yield file paths."""
    for dirpath, _, filenames in os.walk(root_path):
        for filename in filenames:
            yield os.path.join(dirpath, filename)

def initialize_database(config: ConfigParser) -> None:
    """Create database table if it doesn't exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {config['DATABASE']['table_name']} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        file_size INTEGER NOT NULL,
        md5_hash TEXT NOT NULL,
        status TEXT DEFAULT 'processed',
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(file_path)
    )
    """
    try:
        with DatabaseManager(config) as cursor:
            cursor.execute(create_table_sql)
    except Exception as e:
        log_error("Failed to initialize database", e)
        raise

def process_file(file_path: str, config: ConfigParser) -> None:
    """Process single file and store metadata in database."""
    try:
        file_name, file_size, md5_hash = get_file_info(file_path)
        
        insert_sql = f"""
        INSERT OR REPLACE INTO {config['DATABASE']['table_name']}
        (file_name, file_path, file_size, md5_hash, status)
        VALUES (?, ?, ?, ?, ?)
        """
        
        with DatabaseManager(config) as cursor:
            cursor.execute(insert_sql, (
                file_name,
                file_path,
                file_size,
                md5_hash,
                'processed'
            ))
            
    except Exception as e:
        log_error(f"Failed to process file {file_path}", e)
        raise

def main() -> None:
    """Main processing function."""
    try:
        # Load configuration
        config = load_configuration()
        
        # Initialize logging
        configure_logging(config)
        logger = logging.getLogger(__name__)
        logger.info("Starting file processing")
        
        # Initialize database
        initialize_database(config)
        
        # Process files
        root_path = config['DEFAULT']['root_path']
        processed_count = 0
        skipped_count = 0
        
        for file_path in walk_directory(root_path):
            try:
                logger.debug(f"Processing file: {file_path}")
                process_file(file_path, config)
                processed_count += 1
            except Exception as e:
                logger.warning(f"Skipped file {file_path} due to error: {str(e)}")
                skipped_count += 1
                continue
                
        logger.info(
            f"File processing completed. "
            f"Processed: {processed_count} files, "
            f"Skipped: {skipped_count} files, "
            f"Total: {processed_count + skipped_count} files"
        )
        
    except Exception as e:
        log_error("Fatal error in main processing", e)
        raise

if __name__ == "__main__":
    main()
