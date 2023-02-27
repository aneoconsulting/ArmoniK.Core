locals {
  env = [
    "Zipkin__Uri=${var.zipkin_uri}",
    "Amqp__PartitionId=${var.unresolved_dependencies_queue}",
  ]
  gen_env = [for k, v in var.generated_env_vars : "${k}=${v}"]
}