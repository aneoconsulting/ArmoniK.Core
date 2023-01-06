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

variable "log_level" {
  type = string
}

variable "dev_env" {
  type = string
}

variable "queue_env_vars" {
  type = map(any)
}

variable "database_env_vars" {
  type = map(any)
}

variable "object_env_vars" {
  type = map(any)
}

variable "object_storage" {
  type = string
}

variable "object_driver" {
  type = object({
    name    = string,
    address = string,
  })
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}
