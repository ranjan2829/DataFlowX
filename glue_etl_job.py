# glue_etl_job.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# PostgreSQL (update with your RDS endpoint later)
jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {"user": "ranjanshahajishitole", "password": "1234", "driver": "org.postgresql.Driver"}
orders_df = spark.read.jdbc(url=jdbc_url, table="orders", properties=properties)

# S3 logs
logs_df = spark.read.csv("s3://data-flow-x/logs/web_logs.csv", header=True)

# Transform
orders_agg = orders_df.groupBy("product").sum("price").withColumnRenamed("sum(price)", "total_sales")
logs_agg = logs_df.groupBy("action").count()

# Write to S3 (for Redshift later)
orders_agg.write.parquet("s3://data-flow-x/processed/orders/")
logs_agg.write.parquet("s3://data-flow-x/processed/logs/")

job.commit()