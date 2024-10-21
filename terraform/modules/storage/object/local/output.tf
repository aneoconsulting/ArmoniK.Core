output "generated_env_vars" {
  value = ({
    "Components__ObjectStorageAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.LocalStorage.ObjectBuilder"
    "Components__ObjectStorageAdaptorSettings__AdapterAbsolutePath" = "/adapters/object/local_storage/ArmoniK.Core.Adapters.LocalStorage.dll"
    "LocalStorage__Path"                                            = var.local_path
  })
}

output "volumes" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value = {
    (docker_volume.object.name) : var.local_path,
  }
}
