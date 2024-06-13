resource "docker_image" "database" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "database" {
  name  = var.mongodb_params.database_name
  image = docker_image.database.image_id

  command = ["--replSet", var.mongodb_params.replica_set_name]

  networks_advanced {
    name = var.network
  }

  mounts {
    type   = "bind"
    target = "/docker-entrypoint-initdb.d/"
    source = abspath("${path.root}/mongodb")
  }

  ports {
    internal = 27017
    external = var.mongodb_params.exposed_port
  }

  wait         = true
  wait_timeout = 30

  healthcheck {
    test         = concat(["CMD", "bash", "-c"], split(" ", local.test-cmd))
    interval     = "10s"
    timeout      = "3s"
    start_period = "10s"
    retries      = "10"
  }

}

resource "null_resource" "partitions_in_db" {

  for_each = var.partition_list
  provisioner "local-exec" {
    command = "docker run --net ${var.network} --rm rtsp/mongosh mongosh mongodb://database:27017/${docker_container.database.name} --eval 'db.PartitionData.insertOne(${jsonencode(each.value)})'"
  }
}
