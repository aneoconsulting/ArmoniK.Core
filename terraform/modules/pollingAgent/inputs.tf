variable "core_tag" {
  type = string
}

variable "container_name" {
  type = string
}

variable "replica_counter" {
  type = number
}

variable "docker_image" {
  type = string
}

variable "network" {
  type = string
}

variable "socket_vol" {
  type = string
}

variable "zipkin_uri" {
  type = string
}

variable "log_driver_name" {
  type = string
}

variable "log_driver_address" {
  type = string
}