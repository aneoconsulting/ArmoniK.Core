module "local_storage" {
  source = "../storage/object/local"
}

resource "docker_image" "dependency_checker" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.docker_image}:${var.core_tag}"
  keep_locally = true
}

module "dependency_checker_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../build_image"
  use_local_image = var.use_local_image
  image_name      = "dependency_checker_local"
  context_path    = "${path.root}/../"
  dockerfile_path = "${path.root}/../Control/DependencyChecker/src/"
}

resource "docker_container" "dependency_checker" {
  name  = var.container_name
  image = one(concat(module.dependency_checker_local, docker_image.dependency_checker)).image_id

  networks_advanced {
    name = var.network
  }

  env = concat(local.env, local.gen_env)

  log_driver = var.log_driver.name

  log_opts = {
    fluentd-address = var.log_driver.address
  }

  ports {
    internal = 1081
    external = 5012
  }
}