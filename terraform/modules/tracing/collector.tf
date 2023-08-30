resource "docker_image" "otel_collector" {
  name         = var.otel_collector_image
  keep_locally = true
}

resource "docker_container" "otel_collector" {
  name  = "otel_collector"
  image = docker_image.otel_collector.image_id

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 4317
    external = var.ingestion_ports.http
  }

  mounts {
    type   = "bind"
    target = "/etc/logs"
    source = abspath("${path.root}/logs")
  }

  mounts {
    type   = "bind"
    target = "/etc/otelcol-contrib/config.yaml"
    source = abspath(local_file.config.filename)
  }
}

locals {
  exporters = [
    for key, value in var.exporters : key if value
  ]
  exporters_conf = {
    file = {
      path = "/etc/logs/traces.json"
    }
    zipkin = {
      endpoint = "http://${docker_container.zipkin.0.name}:${var.ingestion_ports.zipkin}/api/v2/spans"
    }
  }
}

resource "local_file" "config" {
  content = yamlencode({
    receivers = {
      otlp = {
        protocols = {
          grpc = {}
          http = {}
        }
      }
    }
    service = {
      extensions = []
      pipelines = {
        traces = {
          receivers  = ["otlp"]
          processors = []
          exporters  = local.exporters
        }
      }
    }
    exporters = {
      for key, value in local.exporters_conf :
      key => value if contains(local.exporters, key)
    }
  })

  filename = "${path.module}/config.yaml"
}
