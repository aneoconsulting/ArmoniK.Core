variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "exposed_ports" {
  type = object({
    admin_interface = number,
    amqp_connector  = number,
  })
  default = {
    admin_interface = 8161
    amqp_connector  = 5672
  }
}