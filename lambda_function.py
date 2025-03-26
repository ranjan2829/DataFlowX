import json
import boto3
import base64
import logging
import os
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')

# Get environment variables
output_bucket = os.environ.get('OUTPUT_BUCKET', 'dataflowx-processed')
dynamodb_table_name = os.environ.get('DYNAMODB_TABLE', 'dataflowx-analytics')

def lambda_handler(event, context):
    """
    AWS Lambda handler to process Kinesis or Kafka stream data
    """
    logger.info(f"Processing {len(event['Records'])} records")
    
    processed_records = []
    for record in event['Records']:
        try:
            # Process based on event source
            if 'kinesis' in record:
                # Kinesis data is base64 encoded
                payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
                data = json.loads(payload)
            elif 'kafka' in record:
                # Kafka data is also base64 encoded
                payload = base64.b64decode(record['kafka']['value']).decode('utf-8')
                data = json.loads(payload)
            else:
                # Default fallback for other sources
                data = record.get('body', '{}')
                if isinstance(data, str):
                    data = json.loads(data)
            
            # Enrichment: Add sentiment analysis if text data exists
            if 'text' in data or 'message' in data or 'description' in data:
                text_field = 'text' if 'text' in data else ('message' if 'message' in data else 'description')
                text_content = data[text_field]
                
                # Only process if text is not empty and is string
                if text_content and isinstance(text_content, str):
                    # Limit text length for Comprehend (max 5KB)
                    if len(text_content) > 5000:
                        text_content = text_content[:5000]
                    
                    # Get sentiment
                    sentiment_response = comprehend.detect_sentiment(
                        Text=text_content,
                        LanguageCode='en'
                    )
                    
                    # Add sentiment data
                    data['sentiment'] = sentiment_response['Sentiment']
                    data['sentiment_score'] = sentiment_response['SentimentScore']
            
            # Add processing metadata
            data['processed_at'] = datetime.now().isoformat()
            data['processor_id'] = context.function_name
            
            # Store in DynamoDB for analytics
            table = dynamodb.Table(dynamodb_table_name)
            table.put_item(Item=data)
            
            # Add to processed records
            processed_records.append(data)
            
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            # Add error record for monitoring
            error_record = {
                'error': str(e),
                'record_id': record.get('eventID', 'unknown'),
                'timestamp': datetime.now().isoformat()
            }
            processed_records.append(error_record)
    
    # Store batch of processed records in S3
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    s3_key = f"processed/{timestamp}-{context.aws_request_id}.json"
    
    s3.put_object(
        Bucket=output_bucket,
        Key=s3_key,
        Body=json.dumps(processed_records),
        ContentType='application/json'
    )
    
    return {
        'statusCode': 200,
        'processed_count': len(processed_records),
        'output_location': f"s3://{output_bucket}/{s3_key}"
    }