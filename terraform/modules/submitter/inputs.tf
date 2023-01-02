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

variable "db_driver" {
  type = object({
    name    = string,
    port    = number,
  })
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
