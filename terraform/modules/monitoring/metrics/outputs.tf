output "metrics_env_vars" {
  value = ({
    "MetricsExporter__Host" = "http://${docker_container.metrics.name}"
    "MetricsExporter__Port" = 1080
    "MetricsExporter__Path" = "/metrics"
  })
}