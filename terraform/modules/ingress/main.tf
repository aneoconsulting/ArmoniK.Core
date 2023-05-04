resource "docker_image" "ingress" {
  name         = "${var.container.image}:${var.container.tag}"
  keep_locally = true
}

resource "docker_container" "ingress" {
  name  = var.container.name
  image = docker_image.ingress.image_id

  networks_advanced {
    name = var.network
  }

  log_driver = var.log_driver.name

  log_opts = {
    fluentd-address = var.submitter.id == "" ? var.log_driver.address : var.log_driver.address
  }

  ports {
    internal = var.tls ? 8443 : 8080
    external = var.tls ? (var.mtls ? 5213 : 5212) : 5211
  }

  ports {
    internal = var.tls ? 9443 : 9080
    external = var.tls ? (var.mtls ? 5203 : 5202) : 5201
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