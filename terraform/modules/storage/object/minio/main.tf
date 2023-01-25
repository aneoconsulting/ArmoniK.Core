resource "docker_image" "object" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "object" {
  name  = var.minio_parameters.host
  image = docker_image.object.image_id
  entrypoint = ["/bin/bash"]
  command = ["-c", "mkdir -p /data/${var.minio_parameters.bucket_name} && minio server /data --console-address :9001"]

  networks_advanced {
    name = var.network
  }

  ports {
    internal = var.minio_parameters.port
    external = var.minio_parameters.port
  }
  ports {
    internal = 9001
    external = 9001
  }
}