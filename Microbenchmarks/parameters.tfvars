# Core configuration (assuming these already exist in your setup)
region  = "us-east-1" 
profile = "default"
prefix  = "armonik-microbench-workflows"

# Additional tags for this deployment
additional_common_tags = {
  "github-run-id" = "12345"
  "environment"   = "workflows"
}

# Benchmark runner configuration
benchmark_runner = {
  instance_type = "c7a.8xlarge"
}

# Enable only the benchmarks you want to run
# Comment out or remove the ones you don't want

# Storage benchmarks
localstorage_benchmark = {
  
}

# redis_benchmark = {
#   instance_type = "cache.m5.xlarge"
# }

# efs_benchmark = {}

# s3_benchmark = {}

# # Queue benchmarks
# sqs_benchmark = {}

# rabbitmq_amq_benchmark = {
#   instance_type     = "mq.m5.4xlarge"
#   username_override = "mybenchuser"
#   password_override = "mybenchpass"
# }

# rabbitmq_ec2_benchmark = {
#   instance_type = "m5.4xlarge"
# }

# activemq_benchmark = {
#   instance_type     = "mq.m5.4xlarge"
#   username_override = "activemquser"
#   password_override = "activemqpass"
# }
