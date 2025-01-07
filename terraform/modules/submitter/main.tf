# Log the value of object_storage.name
resource "null_resource" "log_object_storage_condition" {
  provisioner "local-exec" {
    command = <<EOT
      echo "###############################"
      echo "Current object storage: ${var.object_storage.name}"
      echo "###############################"
      if [ "${var.object_storage.name}" = "redis" ]; then
        echo "Redis is selected as the object storage.";
      else
        echo "Redis is NOT selected. Current object storage is: ${var.object_storage.name}";
      fi
    EOT
  }
}



# Docker image for the Submitter
resource "docker_image" "submitter" {
  name         = "${var.docker_image}:${var.core_tag}"
  keep_locally = true
}

# Docker container for the Submitter
resource "docker_container" "submitter" {  
  name  = var.container_name
  image = docker_image.submitter.image_id

  networks_advanced {
    name = var.network
  }

  env = concat(local.env, local.gen_env)

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

  # Conditional mounting for Redis certificates
  dynamic "mounts" {
    for_each = var.object_storage.name == "redis" ? local.redis_cert_mounts : {}

    content {
      type   = "bind"
      source = mounts.key
      target = mounts.value
    }
  }

  # Mount volumes
  dynamic "mounts" {
    for_each = var.volumes

    content {
      type   = "volume"
      source = mounts.key
      target = mounts.value
    }
  }

  # Upload files if necessary
  dynamic "upload" {
    for_each = var.mounts

    content {
      source = upload.value
      file   = upload.key
    }
  }
}
