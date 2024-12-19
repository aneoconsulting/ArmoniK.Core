output "generated_env_vars" {
  value = ({
    "Components__ObjectStorageAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Redis.ObjectBuilder"
    "Components__ObjectStorageAdaptorSettings__AdapterAbsolutePath" = "/adapters/object/redis/ArmoniK.Core.Adapters.Redis.dll"
    "Redis__EndpointUrl"                                            = "object:${var.exposed_port}"
  })
}
output "volumes" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value       = {}
}
