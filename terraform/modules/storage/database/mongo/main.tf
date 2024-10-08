resource "docker_image" "database" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "database" {
  name  = var.mongodb_params.database_name
  image = docker_image.database.image_id

  command = ["mongod", "--bind_ip_all", "--replSet", var.mongodb_params.replica_set_name]

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 27017
    external = var.mongodb_params.exposed_port
  }

  wait = !var.mongodb_params.windows

  dynamic "healthcheck" {
    for_each = var.mongodb_params.windows ? [] : [1]
    content {
      test     = ["CMD", "mongosh", "--quiet", "--eval", "db.runCommand('ping').ok"]
      interval = "3s"
      retries  = "2"
      timeout  = "2s"
    }
  }
}

resource "time_sleep" "wait" {
  create_duration = var.mongodb_params.windows ? "15s" : "0s"
  depends_on      = [docker_container.database]
}

locals {
  linux_run = "docker run --net ${var.network} ${docker_image.database.image_id} mongosh mongodb://${docker_container.database.name}:27017/${var.mongodb_params.database_name}"
  // mongosh is not installed in windows docker images so we need it to be installed locally
  windows_run = "mongosh.exe mongodb://localhost:${var.mongodb_params.exposed_port}/${var.mongodb_params.database_name}"
  prefix_run  = var.mongodb_params.windows ? local.windows_run : local.linux_run
}

resource "null_resource" "init_replica" {
  provisioner "local-exec" {
    command = "${local.prefix_run} --eval 'rs.initiate()'"
  }
  depends_on = [time_sleep.wait]
}

resource "null_resource" "partitions_in_db" {
  for_each = var.partition_list
  provisioner "local-exec" {
    command = "${local.prefix_run} --eval 'db.PartitionData.insertOne(${jsonencode(each.value)})'"
  }
  depends_on = [null_resource.init_replica]
}
