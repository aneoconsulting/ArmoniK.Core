output "generated_env_vars" {
  value = {
    "Components__TableStorage"                  = "ArmoniK.Adapters.PostgresSQL.TableStorage"
    "Components__AuthenticationStorage"         = "ArmoniK.Adapters.PostgresSQL.AuthenticationTable"
    "PostgreSQL__Host"                          = docker_container.database.name
    "PostgreSQL__Port"                          = "5432"
    "PostgreSQL__User"                          = var.postgresql_params.user
    "PostgreSQL__Password"                      = var.postgresql_params.password
    "PostgreSQL__DatabaseName"                  = var.postgresql_params.database_name
    "PostgreSQL__Ssl"                           = tostring(var.postgresql_params.ssl)
    "PostgreSQL__MaxPoolSize"                   = tostring(var.postgresql_params.max_pool_size)
    "PostgreSQL__TableStorage__PollingDelayMin" = var.postgresql_params.min_polling_delay
    "PostgreSQL__TableStorage__PollingDelayMax" = var.postgresql_params.max_polling_delay
  }
}

output "core_mounts" {
  value = var.postgresql_params.ssl ? {
    "/postgresql-certificate/ca.pem" = local_sensitive_file.ca[0].filename
  } : {}
}
