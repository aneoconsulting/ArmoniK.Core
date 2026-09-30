resource "docker_image" "object" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "object" {
  name    = var.host
  image   = docker_image.object.image_id
  command = ["mini", "-dir=/data", "-s3.port=${var.port}", "-bucket=${var.bucket_name}", "-master.telemetry=false"]
  env = [
    "AWS_ACCESS_KEY_ID=${var.login}",
    "AWS_SECRET_ACCESS_KEY=${var.password}",
  ]

  networks_advanced {
    name = var.network.name
  }
  network_mode = var.network.driver

  ports {
    internal = var.port
    external = var.port
  }
}
