output "generated_env_vars" {
  value = ({
    "Components__ObjectStorage" = "ArmoniK.Adapters.LocalStorage.ObjectStorage",
    "LocalStorage__Path"        = var.local_path
  })
}

output "volumes" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value = {
    (docker_volume.object.name) : var.local_path,
  }
}
