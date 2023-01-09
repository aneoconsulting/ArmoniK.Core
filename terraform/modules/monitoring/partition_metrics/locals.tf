locals {
  env = [
    "Serilog__MinimumLevel=${var.log_level}",
    "ASPNETCORE_ENVIRONMENT=${var.dev_env}",
  ]
  db_env      = [for k, v in var.database_env_vars : "${k}=${v}"]
  metrics_env = [for k, v in var.metrics_env_vars : "${k}=${v}"]
  gen_env     = concat(local.db_env, local.metrics_env)
}