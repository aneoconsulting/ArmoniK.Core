output "generated_env_vars" {
  value = ({
    "Redis__Host"                                                   = "redis"
    "Redis__Port"                                                   = 6380
    "Redis__Ssl"                                                    = true
    "Redis__Scheme"                                                 = "redis"
    "Redis__CaPath"                                                 = "/redis/certs/ca.pem"
    "Redis__CertPath"                                               = "/redis/certs/redis.crt"
    "Redis__KeyPath"                                                = "/redis/certs/redis.key"
    "Redis_Timeout"                                                 = "20000"
    "Components__ObjectStorage"                                     = "ArmoniK.Adapters.Redis.ObjectStorage",
    "Components__ObjectStorageAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Redis.ObjectBuilder"
    "Components__ObjectStorageAdaptorSettings__AdapterAbsolutePath" = "/adapters/object/redis/ArmoniK.Core.Adapters.Redis.dll"
    "Redis__EndpointUrl"                                            = "redis:6380"
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