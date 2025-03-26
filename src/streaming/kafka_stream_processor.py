from kafka import KafkaConsumer, KafkaProducer
import json
import logging
from datetime import datetime

class KafkaStreamProcessor:
    """Component to process streaming data from Kafka."""
    
    def __init__(self, config_path):
        """Initialize with configuration."""
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        self.logger = logging.getLogger(__name__)
        
    def create_consumer(self, topic, group_id=None):
        """Create a Kafka consumer."""
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.config["bootstrap_servers"],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=group_id or self.config.get("consumer_group", "dataflowx-group"),
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            return consumer
        except Exception as e:
            self.logger.error(f"Failed to create Kafka consumer: {e}")
            raise
    
    def create_producer(self):
        """Create a Kafka producer."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.config["bootstrap_servers"],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
        except Exception as e:
            self.logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def process_stream(self, input_topic, output_topic, transform_function):
        """Process a stream by applying a transform function and sending to output topic."""
        consumer = self.create_consumer(input_topic)
        producer = self.create_producer()
        
        self.logger.info(f"Starting stream processing from {input_topic} to {output_topic}")
        
        try:
            for message in consumer:
                # Apply the transformation
                transformed_data = transform_function(message.value)
                
                # Add metadata
                transformed_data["processed_at"] = datetime.now().isoformat()
                
                # Send to output topic
                producer.send(output_topic, transformed_data)
                
                self.logger.debug(f"Processed message: {message.value['id'] if 'id' in message.value else 'unknown'}")
        
        except KeyboardInterrupt:
            self.logger.info("Stream processing stopped by user")
        except Exception as e:
            self.logger.error(f"Error in stream processing: {e}")
            raise
        finally:
            producer.close()