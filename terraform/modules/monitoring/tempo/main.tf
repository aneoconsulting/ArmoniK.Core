resource "docker_image" "tempo" {
  name         = var.image
  keep_locally = true
}
resource "docker_container" "tempo" {
  name  = "tempo"
  image = docker_image.tempo.image_id

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 3200
    external = var.exposed_ports.tempo
  }

  ports {
    internal = 4317
    external = var.exposed_ports.oltp_grpc
  }

  ports {
    internal = 4318
    external = var.exposed_ports.oltp_http
  }

  #ports {
  #internal = 55681
  #external = var.exposed_ports.unknown
  #}

  mounts {
    type   = "bind"
    target = "/etc/tempo.yaml"
    source = abspath("${path.root}/tempo/tempo.yaml")
  }

}