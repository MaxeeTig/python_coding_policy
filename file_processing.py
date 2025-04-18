
"""
File Processing System
Recursively processes files in directories, calculates checksums and stores metadata
"""

import os
import hashlib
import logging
from typing import Dict, Optional
from python_coding_20250418 import (
    load_configuration,
    configure_logging,
    DatabaseManager,
    log_error
)

# Database schema constants
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS file_metadata (
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

def calculate_md5(file_path: str, chunk_size: int = 8192) -> str:
    """
    Calculate MD5 hash of a file in chunks to handle large files
    """
    md5 = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                md5.update(chunk)
        return md5.hexdigest()
    except Exception as e:
        raise RuntimeError(f"Failed to calculate MD5 for {file_path}") from e

def process_file(file_path: str, cursor: sqlite3.Cursor) -> None:
    """
    Process a single file and store its metadata in database
    """
    try:
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        md5_hash = calculate_md5(file_path)
        
        cursor.execute(
            """
            INSERT OR REPLACE INTO file_metadata 
            (file_name, file_path, file_size, md5_hash, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (file_name, file_path, file_size, md5_hash, 'processed')
        )
        logging.debug(f"Processed file: {file_path}")
    except Exception as e:
        log_error(f"Error processing file {file_path}", e)
        raise

def process_directory(root_path: str, cursor: sqlite3.Cursor) -> None:
    """
    Recursively process all files in directory tree
    """
    try:
        for dirpath, _, filenames in os.walk(root_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                process_file(file_path, cursor)
    except Exception as e:
        log_error(f"Error processing directory {root_path}", e)
        raise

def initialize_database(cursor: sqlite3.Cursor) -> None:
    """
    Create database tables if they don't exist
    """
    cursor.execute(CREATE_TABLE_SQL)

def main() -> None:
    """
    Main processing function
    """
    try:
        # Load configuration
        config = load_configuration()
        
        # Initialize logging
        configure_logging(config)
        logger = logging.getLogger(__name__)
        logger.info("Starting file processing")
        
        # Process files
        with DatabaseManager(config) as cursor:
            initialize_database(cursor)
            root_path = config['DEFAULT']['root_path']
            process_directory(root_path, cursor)
            
        logger.info("File processing completed successfully")
        
    except Exception as e:
        log_error("File processing failed", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
