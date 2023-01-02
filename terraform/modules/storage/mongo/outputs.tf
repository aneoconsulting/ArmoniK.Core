
output "database_driver" {
  value = ({
    name = docker_container.database.name,
    port = var.exposed_port
  })
}