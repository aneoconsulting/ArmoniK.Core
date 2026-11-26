locals {
  container_name = "oauth2-proxy"
}

resource "docker_image" "oauth2_proxy" {
  name         = var.image
  keep_locally = true
}

# Authentication service of the ingress running the Proxy method, consulted through nginx auth_request: it validates
# the bearer token of the caller against the issuer and answers its user (sub) and groups in X-Auth-Request-*. Test
# deployment: callers present tokens, no browser session is used, hence the fixed cookie secret
resource "docker_container" "oauth2_proxy" {
  name  = local.container_name
  image = docker_image.oauth2_proxy.image_id

  networks_advanced {
    name = var.network.name
  }
  network_mode = var.network.driver

  log_driver = var.log_driver.name
  log_opts   = var.log_driver.log_opts

  command = [
    "--http-address=0.0.0.0:4180",
    "--provider=oidc",
    "--oidc-issuer-url=${var.issuer}",
    "--client-id=${var.client_id}",
    "--client-secret=unused-without-browser-sessions",
    "--cookie-secret=armonik-test-cookie-secret-32byt",
    "--cookie-secure=false",
    "--email-domain=*",
    "--upstream=static://202",
    "--set-xauthrequest=true",
    "--skip-jwt-bearer-tokens=true",
    "--oidc-groups-claim=${var.groups_claim}",
  ]

  # It exits when the issuer cannot be reached at startup
  restart = "on-failure"
}
