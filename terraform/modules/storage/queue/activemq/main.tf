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

  ports {
    internal = 5672
    external = var.exposed_ports.amqp_connector
  }

  ports {
    internal = 8161
    external = var.exposed_ports.admin_interface
  }

  healthcheck {
    test         = ["CMD-SHELL", "curl -f -u admin:admin -s http://localhost:8161/api/jolokia/exec/org.apache.activemq:type=Broker,brokerName=localhost,service=Health/healthStatus -H Origin:http://localhost | grep Good || exit 1 && echo $$?"]
    interval     = "10s"
    timeout      = "5s"
    start_period = "20s"
    retries      = "5"
  }

  mounts {
    type   = "bind"
    target = "/opt/activemq/conf/jetty.xml"
    source = abspath("${path.root}/activemq/jetty.xml")
  }

  mounts {
    type   = "bind"
    target = "/opt/activemq/conf/activemq.xml"
    source = abspath("${path.root}/activemq/activemq.xml")
  }
}