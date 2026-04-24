variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
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
  })
}

variable "queue_list" {
  type = list(string)
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
