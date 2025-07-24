locals {
  query_parameters = compact([
    var.mongodb_params.use_direct_connection ? "directConnection=true" : null,
    can(coalesce(var.mongodb_params.replica_set_name)) ? "replicaSet=${var.mongodb_params.replica_set_name}" : null,
    "tls=true",
    "tlsInsecure=true",
  ])

  query_suffix = length(local.query_parameters) > 0 ? "?${join("&", local.query_parameters)}" : ""

  connection_string = "mongodb://${docker_container.database.name}:${var.mongodb_params.exposed_port}/${docker_container.database.name}${local.query_suffix}"
}

output "generated_env_vars" {
  value = {
    "Components__TableStorage"               = "ArmoniK.Adapters.MongoDB.TableStorage"
    "MongoDB__ConnectionString"              = local.connection_string
    "MongoDB__TableStorage__PollingDelayMin" = "${var.mongodb_params.min_polling_delay}"
    "MongoDB__TableStorage__PollingDelayMax" = "${var.mongodb_params.max_polling_delay}"
    "MongoDB__AllowInsecureTls"              = "true"
    "MongoDB__CAFile"                        = "/mongo-certificate/ca.pem"
    "MongoDB__ServerSelectionTimeout"        = "00:00:20"
  }

  depends_on = [null_resource.init_replica]
}

output "core_mounts" {
  value = {
    "/mongo-certificate/ca.pem" = local_sensitive_file.ca.filename
  }
}
