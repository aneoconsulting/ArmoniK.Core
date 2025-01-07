resource "docker_image" "object" {
  name         = var.image
  keep_locally = true
}

resource "docker_volume" "redis_certs" {
  name = "redis_certs"
}
resource "local_file" "redis_conf" {
  filename = abspath("${path.module}/redis.conf")
  content  = var.redis_params.windows ? local.windows_redis_conf : local.linux_redis_conf
}


locals {
  linux_run_create_dirs     = <<EOT
    mkdir -p ${path.module}/logs && chmod 777 ${path.module}/logs
    mkdir -p ${path.module}/data && chmod 755 ${path.module}/data
    mkdir -p ${path.module}/generated/redis/certs && chmod 755 ${path.module}/generated/redis/certs
  EOT
  windows_run_create_dirs   = <<EOT
    mkdir ${path.module}\\logs 
    mkdir ${path.module}\\data 
    mkdir ${path.module}\\generated\\redis\\certs
  EOT
  linux_run_prepare_certs   = <<EOT
      echo "Preparing Redis certificates on Linux..."
      docker run --rm \
        -v ${docker_volume.redis_certs.name}:/redis/certs \
        -v ${abspath("${path.module}/generated/redis/certs")}:/certs:ro \
        alpine sh -c "
        if [ -f /certs/redis.key ] && [ -f /certs/redis.crt ] && [ -f /certs/ca.pem ]; then \
          cp /certs/* /redis/certs/ && \
          chown -R 1000:1000 /redis/certs && \
          chmod 600 /redis/certs/redis.key && \
          chmod 644 /redis/certs/redis.crt && \
          chmod 644 /redis/certs/ca.pem; \
        else \
          echo 'Missing certificates'; exit 1; \
        fi"
    EOT
  windows_run_prepare_certs = <<EOT
      echo "Preparing Redis certificates on Windows..."
      copy ${path.module}\\generated\\redis\\certs\\* ${docker_volume.redis_certs.name}:
    EOT
  linux_redis_data          = <<EOT
    mkdir -p /data && chmod 755 /data
    EOT
  windows_redis_data        = <<EOT
    mkdir ${path.module}\\data
    EOT
  linux_redis_conf          = <<EOT
    bind 0.0.0.0
    tls-port 6380
    tls-cert-file /redis/certs/redis.crt
    tls-key-file /redis/certs/redis.key
    tls-ca-cert-file /redis/certs/ca.pem
    tls-auth-clients no
    logfile /redis/logs/redis.log
    tls-protocols "TLSv1.2 TLSv1.3"
    EOT

  windows_redis_conf = <<EOT
      bind 0.0.0.0
      tls-port 6380
      tls-cert-file C:\\redis\\certs\\redis.crt
      tls-key-file C:\\redis\\certs\\redis.key
      tls-ca-cert-file C:\\redis\\certs\\ca.pem
      tls-auth-clients no
      logfile C:\\redis\\logs\\redis.log
      tls-protocols "TLSv1.2 TLSv1.3"
    EOT
}


resource "null_resource" "init_dirs" {
  provisioner "local-exec" {
    command = var.redis_params.windows ? local.windows_run_create_dirs : local.linux_run_create_dirs
  }
}
resource "null_resource" "prepare_certs" {
  provisioner "local-exec" {
    command = var.redis_params.windows ? local.windows_run_prepare_certs : local.linux_run_prepare_certs

  }
  depends_on = [local_file.ca, local_file.cert, local_file.key]
}

resource "docker_container" "object" {
  name  = "redis"
  image = docker_image.object.image_id
  user  = "1000:1000"

  networks_advanced {
    name = var.network
  }

  command = [
    "redis-server",
    var.redis_params.windows ? "C:\\redis\\conf\\redis.conf" : "/etc/redis/redis.conf"
  ]

  ports {
    internal = 6380
    external = 6380
  }

  mounts {
    target    = "/redis/certs"
    source    = docker_volume.redis_certs.name
    type      = "volume"
    read_only = false
  }

   mounts {
    target    = var.redis_params.windows ? "C:\\redis\\conf\\redis.conf" : "/etc/redis/redis.conf"
    source    =  abspath("${path.module}/redis.conf")
    type      = "bind"
    read_only = true
  }

  mounts {
    target    = var.redis_params.windows ? "C:\\redis\\logs" : "/redis/logs"
    source    = var.redis_params.windows ? "${abspath(path.module)}\\logs" : "${abspath(path.module)}/logs" 
    type      = "bind"
    read_only = false
  }

  mounts {
    target    = var.redis_params.windows ? "C:\\redis\\data" : "/data"
    source    = var.redis_params.windows ? "${abspath(path.module)}\\data" : "${abspath(path.module)}/data" 
    type      = "bind"
    read_only = false
  }

  dynamic "healthcheck" {
    for_each = var.object_storage.name == "redis" ? [1] : []
    content {
      test     = ["CMD-SHELL", "redis-cli -p 6380 -a \"$REDIS_PASSWORD\" --tls --cacert /redis/certs/ca.pem --cert /redis/certs/redis.crt --key /redis/certs/redis.key ping | grep PONG"]
      interval = "3s"
      timeout  = "5s"
      retries  = 5
    }
  }

  depends_on = [
    docker_image.object,
   null_resource.init_dirs,
    null_resource.prepare_certs,
    local_file.redis_conf
  ]
}

output "container_id" {
  value       = docker_container.object.id
  description = "The ID of the Redis container"
}
