variable "core_tag" {
  type = string
}

variable "container_name" {
  type = string
}

variable "docker_image" {
  type = string
}

variable "use_local_image" {
  type    = bool
  default = false
}

variable "network" {
  type = string
}

variable "zipkin_uri" {
  type = string
}

variable "generated_env_vars" {
  type = map(string)
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}

variable "unresolved_dependencies_queue" {
  type = string
}
