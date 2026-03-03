# locals {
#   query_parameters = compact([
#     var.mongodb_params.use_direct_connection ? "directConnection=true" : null,
#     can(coalesce(var.mongodb_params.replica_set_name)) ? "replicaSet=${var.mongodb_params.replica_set_name}" : null,
#     "tls=true",
#     "tlsInsecure=true",
#   ])

#   query_suffix = length(local.query_parameters) > 0 ? "?${join("&", local.query_parameters)}" : ""

#   connection_string = "mongodb://${docker_container.database.name}:${var.mongodb_params.exposed_port}/${docker_container.database.name}${local.query_suffix}"
# }

output "generated_env_vars" {
  value = {
    "Components__TableStorage"               = "ArmoniK.Adapters.TaskDB.TableStorage"
    "TaskDB__Host" = "host.docker.internal"
  }

  # depends_on = [null_resource.init_replica]
}

# output "core_mounts" {
#   value = {
#     "/mongo-certificate/ca.pem" = local_sensitive_file.ca.filename
#   }
# }

output "core_mounts" {
  value = {}
}
