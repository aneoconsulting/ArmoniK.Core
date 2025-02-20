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
  env = [
    "Components__QueueAdaptorSettings__ClassName=${"ArmoniK.Core.Adapters.Amqp.QueueBuilder"}",
    "Components__QueueAdaptorSettings__AdapterAbsolutePath=${path.root}/Adaptors/Amqp/src/bin/Debug/net8.0/ArmoniK.Core.Adapters.Amqp.dll",
    "Amqp__User=guest",
    "Amqp__Password=guest",
    "Amqp__Host=localhost",
    "Amqp__Port=${var.queue_envs.port}",
    "Amqp__Scheme=amqp",
    "Amqp__PartitionId=TestPartition",
    "Amqp__MaxPriority=${var.queue_envs.max_priority}",
    "Amqp__MaxRetries=${var.queue_envs.max_retries}",
    "Amqp__LinkCredit=${var.queue_envs.link_credit}",
    "Amqp__AllowHostMismatch=false"
  ]
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