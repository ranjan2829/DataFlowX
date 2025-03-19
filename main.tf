# main.tf
provider "aws" { region = "us-east-1" }

resource "aws_s3_bucket" "data_lake" { bucket = "data-flow-x" }
resource "aws_msk_cluster" "kafka" {
  cluster_name = "ecommerce-kafka"
  kafka_version = "2.8.1"
  number_of_broker_nodes = 2
  broker_node_group_info {
    instance_type = "kafka.m5.large"
    client_subnets = ["subnet-xxx", "subnet-yyy"]  # Replace with your VPC subnets
    security_groups = ["sg-zzz"]  # Replace with your security group
  }
}
resource "aws_emr_cluster" "spark" {
  name = "ecommerce-spark"
  release_label = "emr-6.9.0"
  applications = ["Spark"]
  master_instance_group { instance_type = "m5.xlarge" instance_count = 1 }
  core_instance_group { instance_type = "m5.xlarge" instance_count = 2 }
}
resource "aws_keyspaces_keyspace" "cassandra" { name = "ecommerce" }
resource "aws_redshift_cluster" "redshift" {
  cluster_identifier = "ecommerce-analytics"
  node_type = "dc2.large"
  master_username = "admin"
  master_password = "SecurePass123!"
}