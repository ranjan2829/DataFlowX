


import psycopg2
from faker import Faker
import csv
from kafka import KafkaProducer
import json
import time

fake = Faker()

# Kafka producer (local setup)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
conn = psycopg2.connect(
    dbname="postgres", user="ranjanshahajishitole", password="1234", host="localhost", port="5432"
)
cur = conn.cursor()

# PostgreSQL connection (local setup)

# Create orders table
cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id VARCHAR(50),
        product VARCHAR(100),
        price FLOAT,
        order_date TIMESTAMP
    );
""")

# Generate and insert batch data into PostgreSQL
print("Generating batch data for PostgreSQL...")
for _ in range(100):
    cur.execute(
        "INSERT INTO orders (customer_id, product, price, order_date) VALUES (%s, %s, %s, %s)",
        (fake.uuid4(), fake.word(), fake.random_int(10, 500), fake.date_time_this_year())
    )
conn.commit()

# Generate CSV file for batch logs
print("Generating CSV logs...")
with open('web_logs.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['timestamp', 'user_id', 'action'])
    for _ in range(100):
        writer.writerow([fake.date_time_this_month(), fake.uuid4(), fake.random_element(['click', 'view'])])

# Stream data to Kafka
print("Streaming data to Kafka...")
for _ in range(100):
    stream_data = {
        'user_id': fake.uuid4(),
        'action': fake.random_element(['click', 'purchase']),
        'product': fake.word(),
        'price': fake.random_int(5, 500),
        'timestamp': fake.date_time_this_month().isoformat()
    }
    producer.send('ecommerce-stream', json.dumps(stream_data).encode('utf-8'))
    time.sleep(0.1)  # Simulate real-time streaming

# Cleanup
cur.close()
conn.close()
print("Local data generation complete!")