variable "container_name" {
  type = string
}

variable "docker_image" {
  type = string
}

variable "network" {
  type = string
}

variable "tls" {
    type = bool
}

variable "mtls"{
    type = bool
}