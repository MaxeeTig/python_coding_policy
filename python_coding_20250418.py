"""
PYTHON CODING POLICY REFERENCE FILE
Guidelines for consistent, production-quality Python scripts

=== USAGE INSTRUCTIONS ===
1. Include this policy in the LLM's context window
2. Apply these patterns when generating any Python code
3. Add policy compliance comments where appropriate
"""

# === ENCODING ===
"""
Always use UTF-8 encoding for file operation, logging, database 
"""

# === IMPORTS ===
"""
DO NOT IMPORT FUNCTIONS FROM THIS FILE - IT CONTAINS INSTRUCTIONS, 
IF REQUIRED COPY CODE SNIPPETS TO MAIN CODE
"""
import os
import sys
import logging
from typing import Any, Dict, Optional
import sqlite3  # For database operations
import configparser  # For configuration management


# === CONFIGURATION POLICY ===
"""
Use for default path standard common names: 'logs' for log files, 'database' for database files, 'data' for file sources
"""

def load_configuration(config_path: Optional[str] = None) -> configparser.ConfigParser:
    """
    Load configuration with safe defaults
    Policy Requirements:
    1. Externalize all settings to config files
    2. Allow command-line override of config path
    3. Validate required sections exist
    """
    config = configparser.ConfigParser()
    
    # Determine config path
    final_path = config_path if config_path else (
        sys.argv[1] if len(sys.argv) > 1 else 'config.ini'
    )
    
    if not os.path.exists(final_path):
        raise FileNotFoundError(f"Config file not found: {final_path}")
    
    config.read(final_path)
    
    # Validate minimum required sections
    required_sections = {'LOGGING', 'DATABASE'}
    missing = required_sections - set(config.sections())
    if missing:
        raise ValueError(f"Missing required config sections: {missing}")
    
    return config


# === LOGGING POLICY ===
def configure_logging(config: configparser.ConfigParser) -> None:
    """
    Initialize standardized logging
    Policy Requirements:
    1. Structured format with timestamps
    2. Automatic directory creation
    3. Multiple handlers (file + console)
    """
    log_dir = os.path.dirname(config['LOGGING']['file_path'])
    os.makedirs(log_dir, exist_ok=True)  # Safe directory creation
    
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


# === DATABASE POLICY ===
# === GENERAL ===
"""
For file-based databases like sqlite use in python code CREATE table clause with IF NOT EXIST verification  
"""
# === DATABASE OPERATION ===
class DatabaseManager:
    """
    Safe database operations wrapper
    Policy Requirements:
    1. Context manager protocol
    2. Parameterized queries only
    3. Automatic connection cleanup
    """
    def __init__(self, config: configparser.ConfigParser):
        self.db_path = config['DATABASE']['path']
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Enable dict-style access
        return self.conn.cursor()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()


# === ERROR HANDLING POLICY ===
def log_error(context: str, error: Exception) -> None:
    """
    Standardized error reporting
    Policy Requirements:
    1. Full error context capture
    2. Non-blocking when possible
    3. Structured error information
    """
    logger = logging.getLogger(__name__)
    logger.error(
        f"{context} - {type(error).__name__}: {str(error)}",
        exc_info=True  # Include stacktrace
    )


# === SECURITY POLICY ===
def get_secret(key: str) -> str:
    """
    Secure credential handling
    Policy Requirements:
    1. Never hardcode secrets
    2. Prefer environment variables
    3. Fail clearly if missing
    """
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Required environment variable missing: {key}")
    return value


# === TEMPLATE MAIN FUNCTION ===
def main() -> None:
    """Policy-compliant script template"""
    try:
        # 1. Load configuration
        config = load_configuration()
        
        # 2. Initialize logging
        configure_logging(config)
        logger = logging.getLogger(__name__)
        logger.info("Application started")
        
        # 3. Database example
        with DatabaseManager(config) as cursor:
            cursor.execute(
                "SELECT * FROM table WHERE id = ?",  # Parameterized query
                (123,)  # Never use f-strings or % formatting
            )
            data = cursor.fetchall()
        
        # 4. Environment variable example
        api_key = get_secret('API_KEY')
        
    except Exception as e:
        log_error("Main execution failed", e)
        sys.exit(1)


if __name__ == "__main__":
    main()


"""
=== POLICY COMPLIANCE CHECKLIST ===
When reviewing generated code, verify these markers:

[CONFIG] 
- Externalized settings used
- Config path override supported
- Required sections validated

[LOGGING]
- Directory creation handled
- Timestamps included
- Multiple log levels supported

[DATABASE]
- Context managers used
- Parameterized queries
- Proper transaction handling

[SECURITY]
- No hardcoded secrets
- Environment variables for credentials
- Input validation present

[ERRORS]
- Specific exceptions caught
- Errors logged with context
- Resources properly cleaned up

[STRUCTURE]
- Type hints used
- Clear docstrings present
- Modular organization
"""