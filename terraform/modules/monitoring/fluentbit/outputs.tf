output "fluentd_address" {
  value = "${var.mask}:${var.exposed_port}"
}

output "log_driver_name" {
  value = docker_container.fluentbit.name
}