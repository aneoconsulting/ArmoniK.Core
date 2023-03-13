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

  command = [
    "/bin/sh",
    "-c",
    "/opt/amq/bin/artemis create broker --user ${var.queue_envs.user} --password ${var.queue_envs.password} --role admin --name broker --relax-jolokia --http-host 0.0.0.0 --allow-anonymous && cp /armonik/broker.xml broker/etc && broker/bin/artemis run",
  ]

  ports {
    internal = 5672
    external = var.exposed_ports.amqp_connector
  }

  ports {
    internal = 8161
    external = var.exposed_ports.admin_interface
  }

    mounts {
    type   = "bind"
    target = "/armonik"
    source = abspath("${path.root}/artemis")
  }
}