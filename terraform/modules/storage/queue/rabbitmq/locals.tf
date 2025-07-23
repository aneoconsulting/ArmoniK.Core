locals {
  plug       = var.protocol == "amqp1_0" ? ",rabbitmq_amqp1_0" : ""
  is_windows = docker_image.queue.name == "micdenny/rabbitmq-windows:4.1.0"
}
