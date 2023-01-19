output "generated_env_vars" {
  value = ({
    "Components__ObjectStorage" = "ArmoniK.Adapters.Redis.ObjectStorage",
    "Redis__EndpointUrl"        = "object:${var.exposed_port}"
  })
}