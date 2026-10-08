# In-network, for the OIDC discovery of the control plane, over plain HTTP
output "authority" {
  value = "http://${docker_container.keycloak.name}:8080/realms/${local.realm}"
}

# From the host, for a test client fetching a token.
output "token_endpoint" {
  value = "http://localhost:${var.exposed_port}/realms/${local.realm}/protocol/openid-connect/token"
}

output "realm" {
  value = local.realm
}

output "audience" {
  value = local.client_id
}
