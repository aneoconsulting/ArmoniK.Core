locals {
  env = [
    "Submitter__DefaultPartition=TestPartition0",
    "Serilog__MinimumLevel=${var.log_level}",
    "Zipkin__Uri=${var.zipkin_uri}",
    "ASPNETCORE_ENVIRONMENT=${var.dev_env}",
  ]
  db_env     = [for k, v in var.database_env_vars : "${k}=${v}"]
  queue_env  = [for k, v in var.queue_env_vars : "${k}=${v}"]
  object_env = [for k, v in var.object_env_vars : "${k}=${v}"]
  gen_env    = concat(local.object_env, concat(local.db_env, local.queue_env))
}