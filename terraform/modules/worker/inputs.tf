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

variable "implicit_dependencies" {
  type = list(any)
}