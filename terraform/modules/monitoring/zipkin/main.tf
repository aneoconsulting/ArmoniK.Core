resource "docker_image" "zipkin" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "zipkin" {
  name  = "zipkin"
  image = docker_image.zipkin.image_id

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 9411
    external = var.exposed_port
  }
}