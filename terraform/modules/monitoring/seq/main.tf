resource "docker_image" "seq" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "seq" {
  name  = "seq"
  image = docker_image.seq.image_id

  networks_advanced {
    name = var.network
  }

  env = [
    "ACCEPT_EULA=Y",
    "SEQ_FIRSTRUN_NOAUTHENTICATION=true"
  ]

  ports {
    internal = 80
    external = var.exposed_ports.api
  }

  ports {
    internal = 5341
    external = var.exposed_ports.ingestion
  }
}
