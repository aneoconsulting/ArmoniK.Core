output "generated_env_vars" {
  value = {
    "Components__TableStorage"               = "ArmoniK.Adapters.MongoDB.TableStorage"
    "MongoDB__Host"                          = docker_container.database.name
    "MongoDB__Port"                          = "${var.mongodb_params.exposed_port}"
    "MongoDB__DatabaseName"                  = docker_container.database.name
    "MongoDB__MaxConnectionPoolSize"         = "${var.mongodb_params.max_connection_pool_size}"
    "MongoDB__TableStorage__PollingDelayMin" = "${var.mongodb_params.min_polling_delay}"
    "MongoDB__TableStorage__PollingDelayMax" = "${var.mongodb_params.max_polling_delay}"
    "MongoDB__DirectConnection"              = "${var.mongodb_params.use_direct_connection}"
    "MongoDB__ReplicaSet"                    = "${var.mongodb_params.replica_set_name}"
    "MongoDB__Tls"                           = "true"
    "MongoDB__AllowInsecureTls"              = "true"
    "MongoDB__CAFile"                        = "/cert/ca.pem"
    "MongoDB__ServerSelectionTimeout"        = "00:00:20"
  }

  depends_on = [null_resource.partitions_in_db]
}

output "core_mounts" {
  value = {
    "/cert/ca.pem" = local_sensitive_file.ca.filename
  }
}
