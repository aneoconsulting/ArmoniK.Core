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

  wait = true

  ports {
    internal = 4566
    external = var.exposed_ports.connection
  }

  env = [
    "SERVICES=sqs",
    "LOCALSTACK_HOST=queue",
    "AWS_ACCESS_KEY_ID=localkey",
    "AWS_SECRET_ACCESS_KEY=localsecret"
  ]

  healthcheck {
    test         = concat(["CMD", "curl", "-fsSl", "localhost:4566"])
    interval     = "10s"
    timeout      = "3s"
    start_period = "10s"
    retries      = "10"
  }
}
