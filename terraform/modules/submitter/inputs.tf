variable "core_tag" {
  type = string
}

variable "container_name" {
  type = string
}

variable "docker_image" {
  type = string
}

variable "network" {
  type = string
}

variable "generated_env_vars" {
  type = map(string)
}

variable "volumes" {
  type = map(string)
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}

