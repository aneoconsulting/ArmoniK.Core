resource "docker_volume" "socket_vol" {
  name = "socket_vol${var.replica_counter}"
}

resource "docker_image" "worker" {
  name         = var.worker.image
  keep_locally = true
}

resource "docker_container" "worker" {
  name  = "${var.worker.name}${var.replica_counter}"
  image = docker_image.worker.image_id

  networks_advanced {
    name = var.network
  }

  env = concat(["Serilog__Properties__Application=${var.worker.serilog_application_name}"], local.gen_env, local.common_env)

  log_driver = var.log_driver.name
  log_opts   = var.log_driver.log_opts

  restart = "unless-stopped"

  ports {
    internal = 1080
    external = var.worker.port + var.replica_counter
  }

  mounts {
    type   = "volume"
    target = var.polling_agent.shared_socket
    source = docker_volume.socket_vol.name
  }
}

resource "docker_image" "polling_agent" {
  name         = "${var.polling_agent.image}:${var.core_tag}"
  keep_locally = true
}

resource "docker_container" "polling_agent" {
  name  = "${var.polling_agent.name}${var.replica_counter}"
  image = docker_image.polling_agent.image_id

  networks_advanced {
    name = var.network
  }

  env = concat(local.env, local.gen_env, local.common_env)

  log_driver = var.log_driver.name
  log_opts   = var.log_driver.log_opts

  ports {
    internal = 1080
    external = var.polling_agent.port + var.replica_counter
  }

  mounts {
    type   = "volume"
    target = var.polling_agent.shared_socket
    source = docker_volume.socket_vol.name
  }

  restart = "unless-stopped"

  dynamic "mounts" {
    for_each = var.volumes
    content {
      type   = "volume"
      target = mounts.value
      source = mounts.key
    }
  }

  dynamic "upload" {
    for_each = var.mounts
    content {
      source = upload.value
      file   = upload.key
    }
  }

  depends_on = [docker_container.worker]
}
