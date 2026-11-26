resource "docker_image" "ingress" {
  name         = "${var.container.image}:${var.container.tag}"
  keep_locally = true
}

resource "docker_container" "ingress" {
  name  = var.container.name
  image = docker_image.ingress.image_id

  networks_advanced {
    name = var.network.name
  }
  network_mode = var.network.driver

  log_driver = var.log_driver.name
  log_opts   = var.log_driver.log_opts

  # The web port is the gRPC one plus 10
  ports {
    internal = var.tls ? 8443 : 8080
    external = var.port + 10
  }

  ports {
    internal = var.tls ? 9443 : 9080
    external = var.port
  }

  dynamic "upload" {
    for_each = local.server_files
    content {
      file    = upload.key
      content = upload.value
    }
  }
  restart = "on-failure"
}
