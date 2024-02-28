resource "docker_image" "prometheus" {
  name         = var.image
  keep_locally = true
}
resource "docker_container" "prometheus" {
  name  = "prometheus"
  image = docker_image.prometheus.image_id

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 9090
    external = var.exposed_port
  }

  mounts {
    type   = "bind"
    target = "/etc/prometheus/prometheus.yml"
    source = abspath(local_file.config.filename)
  }
}

resource "local_file" "config" {
  content = yamlencode({
    global = {
      scrape_interval     = "15s"
      evaluation_interval = "15s"
    }
    scrape_configs = [
      {
        job_name = "prometheus"
        static_configs = [
          {
            targets = [
              "localhost:9090"
            ]
          }
        ]
      },
      {
        job_name = "armonik.control.metrics"
        static_configs = [
          {
            targets = [
              "armonik.control.metrics:1080"
            ]
          }
        ]
      },
      {
        job_name = "armonik.control.partition_metrics"
        static_configs = [
          {
            targets = [
              "armonik.control.partition_metrics:1080"
            ]
          }
        ]
      },
      {
        job_name = "armonik.compute.polling_agent"
        static_configs = [
          {
            targets = [
              for v in var.polling_agent_names :
              "${v}:1080"
            ]
          }
        ]
      }
    ]
  })

  filename = "${path.module}/prometheus.yml"
}


