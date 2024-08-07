output "log_driver" {
  value = ({
    name    = docker_container.fluentbit.name,
    address = "localhost:${var.exposed_port}"
  })
}
