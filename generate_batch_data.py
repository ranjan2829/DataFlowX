# generate_data.py (updated)
import psycopg2
from faker import Faker
import csv
import boto3
from kafka import KafkaProducer
import json
import time

fake = Faker()
s3 = boto3.client('s3')
producer = KafkaProducer(bootstrap_servers='localhost:9092')  # Update to MSK later

# PostgreSQL connection (your local setup)
conn = psycopg2.connect(
    dbname="postgres", user="ranjanshahajishitole", password="1234", host="localhost", port="5432"
)
cur = conn.cursor()

# Create orders table (your existing code)
cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id VARCHAR(50),
        product VARCHAR(100),
        price FLOAT,
        order_date TIMESTAMP
    );
""")

# Insert fake data (your existing code)
for _ in range(10000):
    cur.execute(
        "INSERT INTO orders (customer_id, product, price, order_date) VALUES (%s, %s, %s, %s)",
        (fake.uuid4(), fake.word(), fake.random_int(10, 500), fake.date_time_this_year())
    )

conn.commit()
cur.close()
conn.close()

# Generate CSV log file and upload to S3 (your existing code)
with open('web_logs.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['timestamp', 'user_id', 'action'])
    for _ in range(10000):
        writer.writerow([fake.date_time_this_month(), fake.uuid4(), fake.random_element(['click', 'view'])])

s3.upload_file('web_logs.csv', 'data-flow-x', 'logs/web_logs.csv')

# Add streaming data to Kafka
for _ in range(10000):
    stream_data = {
        'user_id': fake.uuid4(),
        'action': fake.random_element(['click', 'purchase']),
        'product': fake.word(),
        'price': fake.random_int(5, 500),
        'timestamp': fake.date_time_this_month().isoformat()
    }
    producer.send('ecommerce-stream', json.dumps(stream_data).encode('utf-8'))
    time.sleep(0.1)  # Simulate real-time

print("Batch and streaming data generated.")