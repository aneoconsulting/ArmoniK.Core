resource "docker_image" "database" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "database" {
  name  = var.mongodb_params.database_name
  image = docker_image.database.image_id

  command = ["mongod", "--bind_ip_all", "--replSet", var.mongodb_params.replica_set_name, "--tlsMode=requireTLS", "--tlsDisabledProtocols=TLS1_0", "--tlsCertificateKeyFile=/mongo-certificate/key.pem", "--tlsCAFile=/mongo-certificate/ca.pem", "--tlsAllowConnectionsWithoutCertificates"]

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
      test     = ["CMD", "mongosh", "--quiet", "--tls", "--tlsCAFile", "/mongo-certificate/ca.pem", "--eval", "db.runCommand('ping').ok"]
     interval = "3s"
      retries  = "2"
      timeout  = "3s"
    }
  }

  upload {
    file    = "/mongo-certificate/key.pem"
    content = local.server_key
  }

  upload {
    file    = "/mongo-certificate/ca.pem"
    content = tls_locally_signed_cert.mongodb_certificate.ca_cert_pem
  }
}
resource "time_sleep" "wait" {
  create_duration = var.mongodb_params.windows ? "30s" : "0s"
  depends_on      = [docker_container.database]
}

locals {
  linux_run = "docker exec ${docker_container.database.name} mongosh mongodb://127.0.0.1:27017/${var.mongodb_params.database_name} --tls --tlsCAFile /mongo-certificate/ca.pem"
  // mongosh is not installed in windows docker images so we need it to be installed locally
  windows_run = "mongosh.exe mongodb://127.0.0.1:${var.mongodb_params.exposed_port}/${var.mongodb_params.database_name} --tls --tlsCAFile ${local_sensitive_file.ca.filename}"
  prefix_run  = var.mongodb_params.windows ? local.windows_run : local.linux_run
}

resource "null_resource" "init_replica" {
  provisioner "local-exec" {
    command = "${local.prefix_run} --eval \"rs.initiate({_id: '${var.mongodb_params.replica_set_name}', members: [{_id: 0, host: '127.0.0.1:27017'}]})\""
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