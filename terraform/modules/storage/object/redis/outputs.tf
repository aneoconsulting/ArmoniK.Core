output "generated_env_vars" {
  value = ({
    "Redis__User"                                                   = var.redis_params.user
    "Redis__Password"                                               = var.redis_params.password
    "Redis__Host"                                                   = var.redis_params.host
    "Redis__Port"                                                   = var.redis_params.port
    "Redis__Ssl"                                                    = var.redis_params.tls_enabled
    "Redis__Scheme"                                                 = "redis"
    "Redis__CaPath"                                                 = "/redis/certs/ca.pem"
    "Redis__CertPath"                                               = "/redis/certs/redis.crt"
    "Redis__KeyPath"                                                = "/redis/certs/redis.key"
    "Redis_Timeout"                                                 = "20000"
    "Redis__EndpointUrl"                                            = "${var.redis_params.host}:${var.redis_params.port}"
    "Components__ObjectStorage"                                     = "ArmoniK.Adapters.Redis.ObjectStorage",
    "Components__ObjectStorageAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Redis.ObjectBuilder"
    "Components__ObjectStorageAdaptorSettings__AdapterAbsolutePath" = "/adapters/object/redis/ArmoniK.Core.Adapters.Redis.dll"
  })
}

output "core_mounts" {
  value = {
    "/redis/certs/ca.pem"    = local_file.ca.filename
    "/redis/certs/redis.crt" = local_file.cert.filename
    "/redis/certs/redis.key" = local_file.key.filename
  }
}

locals {
  redis_endpoints = {
    ip   = "redis"
    port = 6380
  }
}
output "redis_container_id" {
  value = docker_container.object.id
}
output "volumes" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value       = {}
}
