locals {
  plug = var.protocol == "amqp1_0" ? ",rabbitmq_amqp1_0" : ""
}
