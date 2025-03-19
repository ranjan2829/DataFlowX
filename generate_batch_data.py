# generate_batch_data.py
import psycopg2
from faker import Faker
import csv
import boto3

fake = Faker()
s3 = boto3.client('s3')

# PostgreSQL connection (replace with your RDS details)
conn = psycopg2.connect(
    dbname="postgres", user="ranjanshahajishitole", password="1234", host="localhost", port="5432"
)

cur = conn.cursor()

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

# Insert fake data
for _ in range(1000):
    cur.execute(
        "INSERT INTO orders (customer_id, product, price, order_date) VALUES (%s, %s, %s, %s)",
        (fake.uuid4(), fake.word(), fake.random_int(10, 500), fake.date_time_this_year())
    )

conn.commit()
cur.close()
conn.close()

# Generate CSV log file and upload to S3
with open('web_logs.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['timestamp', 'user_id', 'action'])
    for _ in range(1000):
        writer.writerow([fake.date_time_this_month(), fake.uuid4(), fake.random_element(['click', 'view'])])

s3.upload_file('web_logs.csv', 'data-flow-x', 'logs/web_logs.csv')

print("Batch data generated and uploaded.")