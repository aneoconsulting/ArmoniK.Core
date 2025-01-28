resource "docker_image" "object" {
  name         = var.image
  keep_locally = true
}

resource "local_file" "redis_conf" {
  filename = abspath("${path.root}/generated/redis/conf/redis.conf")
  content  = local.linux_redis_conf
}

locals {
  linux_redis_conf = <<EOT
    bind 0.0.0.0
    tls-port 6380
    tls-cert-file /redis/certs/redis.crt
    tls-key-file /redis/certs/redis.key
    tls-ca-cert-file /redis/certs/ca.pem
    tls-auth-clients no
    EOT
}
resource "docker_container" "object" {
  name  = "redis"
  image = docker_image.object.image_id

  command = [
    "redis-server",
    "/etc/redis/redis.conf"
  ]

  networks_advanced {
    name = var.network
  }
  upload {
    file    = "/etc/redis/redis.conf"
    content = local_file.redis_conf.content
  }
  upload {
    file    = "/redis/certs/redis.key"
    content = local_file.key.content
  }
  upload {
    file    = "/redis/certs/redis.crt"
    content = local_file.cert.content
  }

  upload {
    file    = "/redis/certs/ca.pem"
    content = local_file.ca.content
  }
  ports {
    internal = 6380
    external = 6380
  }
  healthcheck {
    test     = ["CMD-SHELL", "redis-cli -p 6380 -a \"$REDIS_PASSWORD\" --tls --cacert /redis/certs/ca.pem --cert /redis/certs/redis.crt --key /redis/certs/redis.key ping | grep PONG"]
    interval = "3s"
    timeout  = "5s"
    retries  = 5
  }

  depends_on = [
    docker_image.object,
    local_file.redis_conf
  ]
}