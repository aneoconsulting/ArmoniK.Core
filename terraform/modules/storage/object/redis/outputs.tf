output "generated_env_vars" {
  value = ({
    "Components__ObjectStorage" = "ArmoniK.Adapters.Redis.ObjectStorage",
    "Redis__EndpointUrl"        = "object:${var.exposed_port}"
  })
}
output "volumes" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value       = {}
}
