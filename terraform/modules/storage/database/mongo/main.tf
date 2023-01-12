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

  for_each = var.partition_list
  provisioner "local-exec" {
    command     = "docker run --net ${var.network} --rm rtsp/mongosh mongosh mongodb://database:27017/${docker_container.database.name} --eval 'db.PartitionData.insertOne(${jsonencode(each.value)})'"
    interpreter = ["/bin/bash", "-c"]
  }
}