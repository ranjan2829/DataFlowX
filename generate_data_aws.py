import boto3
from faker import Faker
from kafka import KafkaProducer
import json
import time

fake = Faker()
s3 = boto3.client('s3')

# Replace with your MSK bootstrap servers (get from AWS MSK console after setup)
msk_bootstrap_servers = 'b-1.your-msk-cluster.xxx.kafka.region.amazonaws.com:9092'
producer = KafkaProducer(bootstrap_servers=msk_bootstrap_servers)

# Generate CSV and upload to S3
print("Generating batch data and uploading to S3...")
with open('web_logs.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['timestamp', 'user_id', 'action'])
    for _ in range(100):
        writer.writerow([fake.date_time_this_month(), fake.uuid4(), fake.random_element(['click', 'view'])])

s3.upload_file('web_logs.csv', 'your-s3-bucket', 'raw/logs/web_logs.csv')

# Stream data to MSK
print("Streaming data to MSK...")
for _ in range(100):
    stream_data = {
        'user_id': fake.uuid4(),
        'action': fake.random_element(['click', 'purchase']),
        'product': fake.word(),
        'price': fake.random_int(5, 500),
        'timestamp': fake.date_time_this_month().isoformat()
    }
    producer.send('ecommerce-stream', json.dumps(stream_data).encode('utf-8'))
    time.sleep(0.1)

print("AWS data generation complete!")