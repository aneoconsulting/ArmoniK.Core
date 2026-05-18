resource "docker_image" "object" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "object" {
  name       = var.host
  image      = docker_image.object.image_id
  entrypoint = ["/bin/sh"]
  command = [
    "-c",
    "mkdir -p /data/${var.bucket_name} && /bin/fake-gcs-server -scheme http -port ${var.port} -public-host ${var.host}:${var.port} -data /data"
  ]

  networks_advanced {
    name = var.network.name
  }
  network_mode = var.network.driver

  ports {
    internal = var.port
    external = var.port
  }
}
