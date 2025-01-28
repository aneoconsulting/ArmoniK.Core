output "generated_env_vars" {
  value = ({
    "Redis__User"                                                   = var.redis_params.user
    "Redis__Password"                                               = var.redis_params.password
    "Redis__Host"                                                   = var.redis_params.host
    "Redis__Port"                                                   = var.redis_params.exposed_port
    "Redis__Ssl"                                                    = var.redis_params.tls_enabled
    "Redis__Scheme"                                                 = var.redis_params.scheme
    "Redis__CaPath"                                                 = var.redis_params.ca_path
    "Redis__CertPath"                                               = var.redis_params.cert_path
    "Redis__KeyPath"                                                = var.redis_params.key_path
    "Redis_Timeout"                                                 = var.redis_params.timeout
    "Components__ObjectStorage"                                     = "ArmoniK.Adapters.Redis.ObjectStorage",
    "Components__ObjectStorageAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Redis.ObjectBuilder"
    "Components__ObjectStorageAdaptorSettings__AdapterAbsolutePath" = "/adapters/object/redis/ArmoniK.Core.Adapters.Redis.dll"
    "Redis__EndpointUrl"                                            = "${var.redis_params.host}:${var.redis_params.exposed_port}"
  })
}
output "volumes" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value       = {}
}

output "core_mounts" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value = {
    "/redis/certs/ca.pem" = local_file.ca.filename
  }
}