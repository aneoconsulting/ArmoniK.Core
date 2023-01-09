resource "docker_volume" "socket_vol" {
  name = "socket_vol${var.replica_counter}"
}

module "local_storage" {
  source = "../storage/object/local"
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
  image_name      = "submitter_local"
  context_path    = "${path.root}../"
  dockerfile_path = "${path.root}../Tests/HtcMock/Server/src" # TODO: Make this a variable ot change worker type
}

resource "docker_container" "worker" {
  name  = "${var.worker.name}${var.replica_counter}"
  image = var.use_local_image ? module.worker_local[0].image_id : docker_image.worker[0].image_id

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

module "pollingagent_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../build_image"
  use_local_image = var.use_local_image
  image_name      = "pollingagent_local"
  context_path    = "${path.root}../"
  dockerfile_path = "${path.root}../Compute/PollingAgent/src/"
}

resource "docker_container" "polling_agent" {
  name  = "${var.polling_agent.name}${var.replica_counter}"
  image = var.use_local_image ? module.pollingagent_local[0].image_id : docker_image.polling_agent[0].image_id

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

  mounts {
    type   = "volume"
    target = "/local_storage"
    source = module.local_storage.object_volume
  }

  healthcheck {
    test         = concat(["CMD", "bash", "-c"], split(" ", local.test-cmd))
    interval     = "5s"
    timeout      = "3s"
    start_period = "20s"
    retries      = 5
  }
}