import sys
import os
import json
import time
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.ingestion.database_ingestion import DatabaseIngestion
from src.processing.data_cleaner import DataCleaner
from src.streaming.kafka_stream_processor import KafkaStreamProcessor

def sample_transform_function(data):
    """Sample transformation function for Kafka stream processing."""
    # Add a timestamp
    data['transformed_at'] = datetime.now().isoformat()
    
    # Convert string fields to lowercase
    for field in ['name', 'category', 'description']:
        if field in data and isinstance(data[field], str):
            data[field] = data[field].lower()
    
    # Add a processed flag
    data['processed'] = True
    
    return data

def run_sample_pipeline():
    """Run a sample data pipeline to test the components."""
    logger.info("Starting sample pipeline...")
    
    try:
        # 1. Setup sample data in PostgreSQL
        setup_sample_data()
        
        # 2. Extract data from database
        db_ingestion = DatabaseIngestion('config/database_config.json')
        extracted_data = db_ingestion.extract_data("SELECT * FROM sample_data LIMIT 100")
        logger.info(f"Extracted {len(extracted_data)} records from database")
        
        # 3. Clean data
        df = pd.DataFrame(extracted_data)
        cleaner = DataCleaner()
        
        cleaning_config = {
            "missing_values": {
                "description": "empty_string",
                "price": "mean"
            },
            "remove_duplicates": True,
            "custom_transforms": {
                "name": "lowercase"
            }
        }
        
        cleaned_df = cleaner.clean_dataframe(df, cleaning_config)
        logger.info(f"Cleaned data shape: {cleaned_df.shape}")
        
        # 4. Send to Kafka (with error handling)
        try:
            kafka_processor = KafkaStreamProcessor('config/kafka_config.json')
            producer = kafka_processor.create_producer()
            
            for _, row in cleaned_df.iterrows():
                # Convert row to dictionary
                record = row.to_dict()
                # Handle non-serializable values
                for key, value in record.items():
                    if isinstance(value, pd.Timestamp):
                        record[key] = value.isoformat()
                    elif pd.isna(value):
                        record[key] = None
                
                # Send to Kafka topic
                producer.send('ecommerce-stream', record)
            
            producer.flush()
            logger.info(f"Sent {len(cleaned_df)} records to Kafka topic 'ecommerce-stream'")
            
            # 5. Process the stream
            logger.info("Starting stream processing (will run for 30 seconds)...")
            
            # Run in a separate thread for demo purposes
            import threading
            stream_thread = threading.Thread(
                target=kafka_processor.process_stream,
                args=('ecommerce-stream', 'processed-events', sample_transform_function)
            )
            stream_thread.daemon = True
            stream_thread.start()
            
            # Let it run for a while
            time.sleep(30)
        except Exception as e:
            logger.error(f"Kafka operations failed: {e}")
            logger.error("Make sure Docker is running with 'docker-compose up -d'")
        
        logger.info("Sample pipeline completed!")
        
    except Exception as e:
        logger.error(f"Error in sample pipeline: {e}")

def setup_sample_data():
    """Create a sample table and insert data for testing."""
    import psycopg2
    
    try:
        # Load database config
        with open('config/database_config.json', 'r') as f:
            config = json.load(f)
        
        conn = psycopg2.connect(
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"]
        )
        
        cursor = conn.cursor()
        
        # Create sample table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sample_data (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(100),
            description TEXT,
            price NUMERIC(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Check if we already have data
        cursor.execute("SELECT COUNT(*) FROM sample_data")
        count = cursor.fetchone()[0]
        
        # Insert sample data if table is empty
        if count == 0:
            logger.info("Inserting sample data...")
            # Create sample product data
            products = [
                ("Smartphone X1", "Electronics", "Latest smartphone with cutting-edge features", 899.99),
                ("Laptop Pro", "Electronics", "High-performance laptop for professionals", 1299.99),
                ("Coffee Maker", "Kitchen", "Automatic coffee maker with timer", 79.99),
                ("Running Shoes", "Sports", "Lightweight running shoes for athletes", 129.99),
                ("Yoga Mat", "Sports", "Non-slip yoga mat", 25.99),
                # Add some records with missing data
                ("Mini Speaker", "Electronics", None, 49.99),
                ("Water Bottle", "Sports", "Insulated water bottle", None),
                ("Desk Lamp", None, "Adjustable LED desk lamp", 35.99),
                # Add duplicates for testing
                ("Smartphone X1", "Electronics", "Latest smartphone with cutting-edge features", 899.99),
                ("Laptop Pro", "Electronics", "High-performance laptop for professionals", 1299.99),
            ]
            
            for product in products:
                cursor.execute(
                    "INSERT INTO sample_data (name, category, description, price) VALUES (%s, %s, %s, %s)",
                    product
                )
            
            conn.commit()
            logger.info(f"Inserted {len(products)} sample products")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Database setup error: {e}")
        logger.error("Make sure PostgreSQL is running via Docker with 'docker-compose up -d'")
        raise

if __name__ == "__main__":
    run_sample_pipeline()