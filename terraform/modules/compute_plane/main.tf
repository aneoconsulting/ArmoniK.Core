resource "docker_volume" "socket_vol" {
  name = "socket_vol${var.replica_counter}"
}

resource "docker_image" "worker" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.worker.image}:${var.core_tag}"
  keep_locally = true
}

module "worker_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../build_image"
  use_local_image = var.use_local_image
  image_name      = "worker_local"
  context_path    = "${path.root}/../"
  dockerfile_path = "${path.root}/../${var.worker.docker_file_path}"
}

resource "docker_container" "worker" {
  name  = "${var.worker.name}${var.replica_counter}"
  image = one(concat(module.worker_local, docker_image.worker)).image_id

  networks_advanced {
    name = var.network
  }

  env = concat(["Serilog__Properties__Application=${var.worker.serilog_application_name}"], local.gen_env)

  log_driver = var.log_driver.name

  log_opts = {
    fluentd-address = var.log_driver.address
  }

  ports {
    internal = 1080
    external = var.worker.port + var.replica_counter
  }

  mounts {
    type   = "volume"
    target = "/cache"
    source = docker_volume.socket_vol.name
  }
}

resource "docker_image" "polling_agent" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.polling_agent.image}:${var.core_tag}"
  keep_locally = true
}

module "polling_agent_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../build_image"
  use_local_image = var.use_local_image
  image_name      = "pollingagent_local"
  context_path    = "${path.root}/../"
  dockerfile_path = "${path.root}/../Compute/PollingAgent/src/"
}

resource "docker_container" "polling_agent" {
  name  = "${var.polling_agent.name}${var.replica_counter}"
  image = one(concat(module.polling_agent_local, docker_image.polling_agent)).image_id

  networks_advanced {
    name = var.network
  }

  env = concat(local.env, local.gen_env)

  log_driver = var.log_driver.name

  log_opts = {
    fluentd-address = var.log_driver.address
  }

  ports {
    internal = 1080
    external = var.polling_agent.port + var.replica_counter
  }

  mounts {
    type   = "volume"
    target = "/cache"
    source = docker_volume.socket_vol.name
  }

  dynamic "mounts" {
    for_each = var.volumes
    content {
      type   = "volume"
      target = mounts.value
      source = mounts.key
    }
  }

  healthcheck {
    test         = ["CMD", "bash", "-c", "exec 3<>\"/dev/tcp/localhost/1080\" && echo -en \"GET /liveness HTTP/1.1\r\nHost: localhost:1080\r\nConnection: close\r\n\r\n\">&3 && grep Healthy <&3 &>/dev/null || exit 1"]
    interval     = "5s"
    timeout      = "3s"
    start_period = "20s"
    retries      = 5
  }

  depends_on = [ docker_container.worker ]
}
