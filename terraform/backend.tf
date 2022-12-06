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

resource "null_resource" "partitions-in-db" {

  provisioner "local-exec" {
    command     = "docker run --net armonik-backend --rm rtsp/mongosh mongosh mongodb://database:27017/database --eval 'db.PartitionData.insertMany([{ _id: \"TestPartition0\", ParentPartitionIds: [], PodReserved: 50, PodMax: 100, PreemptionPercentage: 20, Priority: 1, PodConfiguration: null},{ _id: \"TestPartition1\", ParentPartitionIds: [], PodReserved: 50, PodMax: 100, PreemptionPercentage: 20, Priority: 1, PodConfiguration: null},{ _id: \"TestPartition2\", ParentPartitionIds: [], PodReserved: 50, PodMax: 100, PreemptionPercentage: 20, Priority: 1, PodConfiguration: null}])'"
    interpreter = ["/bin/bash", "-c"]
  }

  depends_on = [
    docker_container.database
  ]
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

  mounts {
    type   = "bind"
    target = "/opt/activemq/conf/jetty.xml"
    source = abspath("./activemq/jetty.xml")
  }

  mounts {
    type   = "bind"
    target = "/opt/activemq/conf/activemq.xml"
    source = abspath("./activemq/activemq.xml")
  }
}