prefix  = "armonik-microbench-workflow"
region  = "us-east-1"
prefix  = "default"


# Benchmark runner configuration
benchmark_runner = {
  instance_type = "c7a.8xlarge"
}

# Benchmark configurations

# Redis
redis_benchmark = {
  instance_type = "cache.m5.xlarge"
}

# EFS
efs_benchmark = {}

# S3
s3_benchmark = {}
