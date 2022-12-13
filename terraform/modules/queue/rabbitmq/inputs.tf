variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "plugins" {
  type    = list(string)
  default = ["rabbitmq_management", "rabbitmq_management_agent", "rabbitmq_amqp1_0"]
}

variable "exposed_ports" {
  type    = list(number)
  default = [5672, 15672]
}