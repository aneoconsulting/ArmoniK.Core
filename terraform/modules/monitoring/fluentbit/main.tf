resource "docker_image" "fluentbit" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "fluentbit" {
  name    = "fluentd"
  image   = docker_image.fluentbit.image_id
  restart = "always"

  networks_advanced {
    name = var.network
  }

  mounts {
    type   = "bind"
    target = "/fluent-bit/etc"
    source = abspath("${path.root}/fluent-bit/etc")
  }

  mounts {
    type   = "bind"
    target = "/logs"
    source = abspath("${path.root}/logs")
  }

  ports {
    internal = 24224
    external = var.exposed_port
    ip       = var.mask
  }

  ports {
    internal = 24224
    external = var.exposed_port
    protocol = "udp"
    ip       = var.mask
  }
}