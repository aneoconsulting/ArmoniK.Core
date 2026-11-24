# In-network address nginx sends its auth subrequests to
output "url" {
  value = "${docker_container.oauth2_proxy.name}:4180"
}
