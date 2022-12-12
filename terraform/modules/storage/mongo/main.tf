resource "docker_image" "database" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "database" {
  name  = "database"
  image = docker_image.database.image_id

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 27017
    external = var.exposed_port
  }
}

resource "null_resource" "partitions_in_db" {

  provisioner "local-exec" {
    command     = "docker run --net ${var.network} --rm rtsp/mongosh mongosh mongodb://database:27017/database --eval 'db.PartitionData.insertMany([{ _id: \"TestPartition0\", ParentPartitionIds: [], PodReserved: 50, PodMax: 100, PreemptionPercentage: 20, Priority: 1, PodConfiguration: null},{ _id: \"TestPartition1\", ParentPartitionIds: [], PodReserved: 50, PodMax: 100, PreemptionPercentage: 20, Priority: 1, PodConfiguration: null},{ _id: \"TestPartition2\", ParentPartitionIds: [], PodReserved: 50, PodMax: 100, PreemptionPercentage: 20, Priority: 1, PodConfiguration: null}])'"
    interpreter = ["/bin/bash", "-c"]
  }

  # TODO: Investigate how to make this implicit
  depends_on = [
    docker_container.database
  ]
}