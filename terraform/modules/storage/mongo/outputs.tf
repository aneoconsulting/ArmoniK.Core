
output "database_driver" { #TODO: to be removed
  value = ({
    name = docker_container.database.name,
    port = var.exposed_port
  })
}

output "database_env_vars" {
  value = ({
    "Components__TableStorage"               = "ArmoniK.Adapters.MongoDB.TableStorage"
    "MongoDB__Host"                          = docker_container.database.name
    "MongoDB__Port"                          = "${var.exposed_port}"
    "MongoDB__DatabaseName"                  = docker_container.database.name
    "MongoDB__MaxConnectionPoolSize"         = "${var.mongodb_params.max_connection_pool_size}"
    "MongoDB__TableStorage__PollingDelayMin" = "${var.mongodb_params.min_polling_delay}"
    "MongoDB__TableStorage__PollingDelayMax" = "${var.mongodb_params.max_polling_delay}"
  })
}