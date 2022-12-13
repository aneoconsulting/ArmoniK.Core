output "log_driver" {
  value = ({
    name = docker_container.fluentbit.name,
    address = "${var.mask}:${var.exposed_port}"
  })
}