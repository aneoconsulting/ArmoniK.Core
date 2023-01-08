resource "docker_image" "object" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "object" {
  name  = "object"
  image = docker_image.object.image_id

  command = ["redis-server"]

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 6379
    external = var.exposed_port
  }
}