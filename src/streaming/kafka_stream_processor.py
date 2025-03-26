from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import time
from datetime import datetime

class KafkaStreamProcessor:
    """Component to process streaming data from Kafka."""
    
    def __init__(self, config_path):
        """Initialize with configuration."""
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        self.logger = logging.getLogger(__name__)
        
    def create_consumer(self, topic, group_id=None, max_retries=3, retry_delay=5):
        """Create a Kafka consumer with retries."""
        retries = 0
        while retries < max_retries:
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.config["bootstrap_servers"],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id=group_id or self.config.get("consumer_group", "dataflowx-group"),
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    # Add a timeout to prevent hanging
                    request_timeout_ms=30000,
                    session_timeout_ms=30000
                )
                self.logger.info(f"Successfully connected to Kafka topic: {topic}")
                return consumer
            except Exception as e:
                retries += 1
                self.logger.warning(f"Failed to connect to Kafka (attempt {retries}/{max_retries}): {e}")
                if retries < max_retries:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"Failed to create Kafka consumer after {max_retries} attempts: {e}")
                    raise
    
    def create_producer(self, max_retries=3, retry_delay=5):
        """Create a Kafka producer with retries."""
        retries = 0
        while retries < max_retries:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.config["bootstrap_servers"],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    # Add a timeout to prevent hanging
                    request_timeout_ms=30000,
                    max_block_ms=30000
                )
                self.logger.info("Successfully connected to Kafka broker")
                return producer
            except Exception as e:
                retries += 1
                self.logger.warning(f"Failed to connect to Kafka (attempt {retries}/{max_retries}): {e}")
                if retries < max_retries:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"Failed to create Kafka producer after {max_retries} attempts: {e}")
                    raise
    
    def process_stream(self, input_topic, output_topic, transform_function, timeout_seconds=30):
        """Process a stream by applying a transform function and sending to output topic."""
        try:
            consumer = self.create_consumer(input_topic)
            producer = self.create_producer()
            
            self.logger.info(f"Starting stream processing from {input_topic} to {output_topic}")
            
            # Set a timeout for the consumer poll
            start_time = time.time()
            records_processed = 0
            
            while (time.time() - start_time) < timeout_seconds:
                # Poll with a timeout to prevent blocking forever
                records = consumer.poll(timeout_ms=1000)
                
                if not records:
                    continue
                    
                for topic_partition, messages in records.items():
                    for message in messages:
                        # Apply the transformation
                        transformed_data = transform_function(message.value)
                        
                        # Add metadata
                        transformed_data["processed_at"] = datetime.now().isoformat()
                        
                        # Send to output topic
                        producer.send(output_topic, transformed_data)
                        records_processed += 1
                        
                        self.logger.debug(f"Processed message: {message.value['id'] if 'id' in message.value else 'unknown'}")
            
            producer.flush()
            self.logger.info(f"Stream processing completed. Processed {records_processed} records.")
            
        except KeyboardInterrupt:
            self.logger.info("Stream processing stopped by user")
        except Exception as e:
            self.logger.error(f"Error in stream processing: {e}")
            raise
        finally:
            if 'producer' in locals():
                producer.close()
            if 'consumer' in locals():
                consumer.close()