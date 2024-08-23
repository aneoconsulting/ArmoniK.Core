output "log_driver" {
  value = ({
    name = docker_container.fluentbit.name,
    log_opts = {
      fluentd-address = "localhost:${var.exposed_port}"
      fluentd-async   = "true"
    }
  })
}
