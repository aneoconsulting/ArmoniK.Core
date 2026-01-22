variable "image" {
  type    = string
  default = "rabbitmq:4-management"
}

variable "network" {
  type = string
}

variable "protocol" {
  type = string
}

variable "queue_envs" {
  type = object({
    user         = string,
    password     = string,
    host         = string,
    port         = number,
    max_priority = number,
    max_retries  = number,
    link_credit  = number,
    partition    = string
  })
}

variable "exposed_ports" {
  type = object({
    admin_interface = number,
    amqp_connector  = number,
  })
  default = {
    admin_interface = 15671
    amqp_connector  = 5671
  }
}