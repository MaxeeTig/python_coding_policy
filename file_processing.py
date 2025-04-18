
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

# Import policy-compliant functions from coding policy
from python_coding_20250418 import (
    load_configuration,
    configure_logging,
    DatabaseManager,
    log_error
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
        for file_path in walk_directory(root_path):
            try:
                logger.debug(f"Processing file: {file_path}")
                process_file(file_path, config)
            except Exception as e:
                logger.warning(f"Skipped file {file_path} due to error: {str(e)}")
                continue
                
        logger.info("File processing completed successfully")
        
    except Exception as e:
        log_error("Fatal error in main processing", e)
        raise

if __name__ == "__main__":
    main()
