locals {
  env = [
    "Serilog__MinimumLevel=${var.log_level}",
    "ASPNETCORE_ENVIRONMENT=${var.dev_env}"
  ]
  db_env = [for t in keys(var.database_env_vars) : format("%s=%s", t, lookup(var.database_env_vars, t))]
}