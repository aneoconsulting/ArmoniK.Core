resource "docker_image" "queue" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "queue" {
  name  = "queue"
  image = docker_image.queue.image_id

  networks_advanced {
    name = var.network
  }

  command = ["gcloud", "beta", "emulators", "pubsub", "start", "--project=plugincore", "--host-port=0.0.0.0:8085"]

  ports {
    internal = 8085
    external = var.exposed_ports.connection
  }
}
