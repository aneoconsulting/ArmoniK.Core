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

  upload {
    file = "/fluent-bit/etc/fluent-bit.conf"
    source = abspath("${path.root}/fluent-bit/etc/fluent-bit.conf")
  }

  upload {
    file = "/fluent-bit/etc/parsers.conf"
    source = abspath("${path.root}/fluent-bit/etc/parsers.conf")
  }

  upload {
    file = "/fluent-bit/etc/append_time.lua"
    source = abspath("${path.root}/fluent-bit/etc/append_time.lua")
  }

  ports {
    internal = 24224
    external = var.exposed_port
  }

  ports {
    internal = 24224
    external = var.exposed_port
    protocol = "udp"
  }
}
