output "url" {
  value = docker_container.submitter.name
}

output "port" {
  value = docker_container.submitter.ports.0.internal
}

output "image_id" {
  value = docker_container.submitter.id
}