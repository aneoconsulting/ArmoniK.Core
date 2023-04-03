resource "docker_image" "prometheus" {
  name         = var.image
  keep_locally = true
}
resource "docker_container" "prometheus" {
  name  = "prometheus"
  image = docker_image.prometheus.image_id

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 9090
    external = var.exposed_port
  }

  mounts {
    type   = "bind"
    target = "/etc/prometheus.yaml"
    source = abspath("${path.root}/prometheus/prometheus.yaml")
  }

}