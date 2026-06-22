resource "docker_image" "database" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "database" {
  name  = var.postgresql_params.database_name
  image = docker_image.database.image_id

  entrypoint = var.postgresql_params.ssl ? [
    "bash", "-c",
    "chmod 600 /postgresql-certificate/server.key && exec /usr/local/bin/docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/postgresql-certificate/server.crt -c ssl_key_file=/postgresql-certificate/server.key -c ssl_ca_file=/postgresql-certificate/ca.pem -c wal_level=logical"
  ] : ["docker-entrypoint.sh", "postgres", "-c", "wal_level=logical"]

  env = [
    "POSTGRES_USER=${var.postgresql_params.user}",
    "POSTGRES_PASSWORD=${var.postgresql_params.password}",
    "POSTGRES_DB=${var.postgresql_params.database_name}",
  ]

  networks_advanced {
    name = var.network.name
  }
  network_mode = var.network.driver

  ports {
    internal = 5432
    external = var.postgresql_params.exposed_port
  }

  wait = !var.windows

  dynamic "healthcheck" {
    for_each = var.windows ? [] : [1]
    content {
      test     = ["CMD-SHELL", "pg_isready -U ${var.postgresql_params.user} -d ${var.postgresql_params.database_name}"]
      interval = "3s"
      retries  = "2"
      timeout  = "3s"
    }
  }

  dynamic "upload" {
    for_each = var.postgresql_params.ssl ? [1] : []
    content {
      file    = "/postgresql-certificate/server.crt"
      content = tls_locally_signed_cert.postgresql_certificate[0].cert_pem
    }
  }

  dynamic "upload" {
    for_each = var.postgresql_params.ssl ? [1] : []
    content {
      file    = "/postgresql-certificate/server.key"
      content = tls_private_key.postgresql_private_key[0].private_key_pem
    }
  }

  dynamic "upload" {
    for_each = var.postgresql_params.ssl ? [1] : []
    content {
      file    = "/postgresql-certificate/ca.pem"
      content = tls_self_signed_cert.root_postgresql[0].cert_pem
    }
  }
}
