locals {
  env = [
    "Submitter__DefaultPartition=TestPartition0",
    "Serilog__MinimumLevel=${var.log_level}",
    "Zipkin__Uri=${var.zipkin_uri}",
    "ASPNETCORE_ENVIRONMENT=${var.dev_env}",
  ]
  db_env     = [for t in keys(var.database_env_vars) : format("%s=%s", t, lookup(var.database_env_vars, t))]
  queue_env  = [for t in keys(var.queue_env_vars) : format("%s=%s", t, lookup(var.queue_env_vars, t))]
  object_env = [for t in keys(var.object_env_vars) : format("%s=%s", t, lookup(var.object_env_vars, t))]
  gen_env    = concat(local.object_env, concat(local.db_env, local.queue_env))
}