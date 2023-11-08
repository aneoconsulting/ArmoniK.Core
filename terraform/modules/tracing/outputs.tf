output "generated_env_vars" {
  value = ({
    "OTLP__Uri" = "http://${docker_container.otel_collector.name}:${var.ingestion_ports.http}"
  })
}
