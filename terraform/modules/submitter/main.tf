resource "docker_image" "submitter" {
  name         = "${var.docker_image}:${var.core_tag}"
  keep_locally = true
}

resource "docker_container" "init" {
  name  = "${var.container_name}_init"
  image = docker_image.submitter.image_id
  count = var.container_init ? 1 : 0

  networks_advanced {
    name = var.network
  }

  env = concat(local.common_env, local.gen_env, local.init_env)

  log_driver = var.log_driver.name
  log_opts   = var.log_driver.log_opts
  must_run   = false

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
}

resource "docker_container" "submitter" {
  name  = var.container_name
  image = docker_image.submitter.image_id

  networks_advanced {
    name = var.network
  }

  env = concat(local.common_env, local.gen_env, local.control_plane_env)

  log_driver = var.log_driver.name
  log_opts   = var.log_driver.log_opts

  ports {
    internal = 1080
    external = 5001
  }

  ports {
    internal = 1081
    external = 5011
  }

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
  depends_on = [docker_container.init]
}
