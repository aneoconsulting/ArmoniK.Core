output "zipkin_uri" {
  value = "http://zipkin:${var.exposed_port}/api/v2/spans"
}