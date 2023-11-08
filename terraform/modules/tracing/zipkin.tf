resource "docker_image" "zipkin" {
  name         = var.zipkin_image
  keep_locally = true
}

resource "docker_container" "zipkin" {
  name  = "zipkin"
  image = docker_image.zipkin.image_id
  count = var.exporters.zipkin ? 1 : 0

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 9411
    external = var.ingestion_ports.zipkin
  }
}
