resource "docker_container" "database" {
  name  = "database"
  image = var.database-image

  networks_advanced {
    name = docker_network.armonik-backend.name
  }

  ports {
    internal = 27017
    external = 27017
  }
}

resource "docker_container" "object" {
  name  = "object"
  image = var.object-image

  command = ["redis-server"]

  networks_advanced {
    name = docker_network.armonik-backend.name
  }

  ports {
    internal = 6379
    external = 6379
  }
}

resource "docker_container" "queue" {
  name  = "queue"
  image = var.queue-image

  networks_advanced {
    name = docker_network.armonik-backend.name
  }

  ports {
    internal = 5672
    external = 5672
  }

  ports {
    internal = 8161
    external = 8161
  }

  healthcheck {
    test         = ["CMD-SHELL", "curl -f -u admin:admin -s http://localhost:8161/api/jolokia/exec/org.apache.activemq:type=Broker,brokerName=localhost,service=Health/healthStatus -H Origin:http://localhost | grep Good || exit 1 && echo $$?"]
    interval     = "10s"
    timeout      = "5s"
    start_period = "20s"
    retries      = "5"
  }
}