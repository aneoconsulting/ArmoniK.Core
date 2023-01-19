output "generated_env_vars" {
  value = ({
    "Components__ObjectStorage" = "ArmoniK.Adapters.LocalStorage.ObjectStorage",
    "LocalStorage__Path"        = "/local_storage"
  })
}

output "object_volume" {
  value = docker_volume.object.name
}