variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "plugins" {
  type = object({
    management       = string,
    management_agent = string,
    protocol1_0      = string
  })
  default = {
    management       = "rabbitmq_management"
    management_agent = "rabbitmq_management_agent"
    protocol1_0      = "rabbitmq_amqp1_0"
  }
}

variable "queue_storage" {
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
    admin_interface = 15672
    amqp_connector  = 5672
  }
}