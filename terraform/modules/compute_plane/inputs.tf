variable "core_tag" {
  type = string
}

variable "polling_agent_container_name" {
  type = string
}

variable "worker_container_name" {
  type = string
}

variable "replica_counter" {
  type = number
}

variable "polling_agent_image" {
  type = string
}

variable "worker_image" {
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

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}