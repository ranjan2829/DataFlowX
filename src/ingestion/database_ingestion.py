import psycopg2
import json
import logging
from datetime import datetime

class DatabaseIngestion:
    """Component to ingest data from relational databases."""
    
    def __init__(self, config_path):
        """Initialize with configuration."""
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        self.logger = logging.getLogger(__name__)
        
    def connect_postgres(self):
        """Establish connection to PostgreSQL database."""
        try:
            conn = psycopg2.connect(
                dbname=self.config["dbname"],
                user=self.config["user"],
                password=self.config["password"],
                host=self.config["host"],
                port=self.config["port"]
            )
            return conn
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    def extract_data(self, query, params=None):
        """Extract data using SQL query."""
        conn = self.connect_postgres()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            self.logger.info(f"Extracted {len(results)} records")
            return results
        except Exception as e:
            self.logger.error(f"Data extraction failed: {e}")
            raise
        finally:
            conn.close()
    
    def extract_incremental(self, table, timestamp_column, last_timestamp=None):
        """Extract data incrementally based on timestamp."""
        if not last_timestamp:
            last_timestamp = datetime(1970, 1, 1).isoformat()
        
        query = f"""
            SELECT * FROM {table}
            WHERE {timestamp_column} > %s
            ORDER BY {timestamp_column} ASC
        """
        return self.extract_data(query, (last_timestamp,))