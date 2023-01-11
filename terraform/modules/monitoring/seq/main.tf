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
    "ACCEPT_EULA=Y"
  ]

  ports {
    internal = 80
    external = 80
  }

  ports {
    internal = 5341
    external = 5341
  }
}
