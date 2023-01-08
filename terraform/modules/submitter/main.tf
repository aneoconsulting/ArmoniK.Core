module "local_storage" {
  source = "../storage/object/local"
}

resource "docker_image" "submitter" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.docker_image}:${var.core_tag}"
  keep_locally = true
}

module "submitter_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../build_image"
  use_local_image = var.use_local_image
  image_name      = "submitter_local"
  context_path    = "${path.root}../"
  dockerfile_path = "${path.root}../Control/Submitter/src/"
}

resource "docker_container" "submitter" {
  name  = var.container_name
  image = var.use_local_image ? module.submitter_local[0].image_id : docker_image.submitter[0].image_id

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
    external = 5001
  }

  ports {
    internal = 1081
    external = 5011
  }

  mounts {
    type   = "volume"
    target = "/local_storage"
    source = module.local_storage.object_volume
  }
}