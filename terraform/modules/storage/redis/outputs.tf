output "object_driver" { #TODO: to be removed
  value = ({
    name    = docker_container.object.name,
    address = "object:${var.exposed_port}"
  })
}

output "object_env_vars" {
  value = ({
    "Components__ObjectStorage" = "ArmoniK.Adapters.Redis.ObjectStorage",
    "Redis__EndpointUrl"        = "object:${var.exposed_port}"
  })
}