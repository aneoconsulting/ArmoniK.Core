output "object_driver" {
  value = ({
    name = docker_container.object.name,
    address = "object:${var.exposed_port}"
  })
}