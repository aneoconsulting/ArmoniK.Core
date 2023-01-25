resource "docker_image" "object" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "object" {
  name  = var.host
  image = docker_image.object.image_id
  entrypoint = ["/bin/bash"]
  command = ["-c", "mkdir -p /data/${var.bucket_name} && minio server /data --console-address :9001"]

  networks_advanced {
    name = var.network
  }

  ports {
    internal = var.port
    external = var.port
  }
  ports {
    internal = 9001
    external = 9001
  }
}