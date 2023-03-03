output "generated_env_vars" {
  value = ({
    "Components__ObjectStorage" = "ArmoniK.Adapters.LocalStorage.ObjectStorage",
    "LocalStorage__Path"        = "/local_storage"
  })
}

output "volumes" {
  description = "Volumes that agents must mount to access the object storage"
  value = {
    (docker_volume.object.name): "/local_storage",
  }
}
