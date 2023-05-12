output "metrics_env_vars" {
  value = ({
    "MetricsExporter__Host" = "http://localhost"
    "MetricsExporter__Port" = var.exposed_port
    "MetricsExporter__Path" = "/metrics"
  })
}