locals {
  env = [
    "Serilog__MinimumLevel=${var.log_level}",
    "ASPNETCORE_ENVIRONMENT=${var.dev_env}",
  ]
  db_env      = [for t in keys(var.database_env_vars) : format("%s=%s", t, lookup(var.database_env_vars, t))]
  metrics_env = [for t in keys(var.metrics_env_vars) : format("%s=%s", t, lookup(var.metrics_env_vars, t))]
  gen_env     = concat(local.db_env, local.metrics_env)
}