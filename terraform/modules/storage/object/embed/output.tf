output "generated_env_vars" {
  value = ({
    "Components__ObjectStorageAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Embed.ObjectBuilder"
    "Components__ObjectStorageAdaptorSettings__AdapterAbsolutePath" = "/adapters/object/embed/ArmoniK.Core.Adapters.Embed.dll"
  })
}

output "volumes" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value       = {}
}

output "core_mounts" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value       = {}
}