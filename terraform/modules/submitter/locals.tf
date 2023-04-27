locals {
  env = [
    "Submitter__DefaultPartition=TestPartition0",
    "Zipkin__Uri=${var.zipkin_uri}",
  ]
  gen_env = [for k, v in var.generated_env_vars : "${k}=${v}"]
}
