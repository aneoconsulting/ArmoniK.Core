resource "docker_image" "grafana" {
  name         = var.image
  keep_locally = true
}
resource "docker_container" "grafana" {
  name  = "grafana"
  image = docker_image.grafana.image_id

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 3000
    external = var.exposed_port
  }

  mounts {
    type   = "bind"
    target = "/etc/grafana/provisioning/datasources/datasources.yaml"
    source = abspath("${path.root}/grafana/datasources.yaml")
  }

}