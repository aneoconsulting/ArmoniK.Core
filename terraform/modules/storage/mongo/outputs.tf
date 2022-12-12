output "db_port" {
  value = var.exposed_port
}

output "network_data" {
  value = docker_container.database.network_data
}