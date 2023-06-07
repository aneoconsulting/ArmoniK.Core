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

  env = [
    "GF_AUTH_ANONYMOUS_ENABLED=true",
    "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin",
    "GF_AUTH_DISABLE_LOGIN_FORM=true"
  ]

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

output "url" {
  value = "http://localhost:${var.exposed_port}"
}