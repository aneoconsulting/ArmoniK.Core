locals {
  realm          = "armonik"
  client_id      = "armonik-client"
  container_name = "keycloak"

  # Client roles of armonik-client, emitted flat in the `roles` claim, become role:<name> principals
  roles_mapper = {
    name           = "roles"
    protocol       = "openid-connect"
    protocolMapper = "oidc-usermodel-client-role-mapper"
    config = {
      "usermodel.clientRoleMapping.clientId" = local.client_id
      "claim.name"                           = "roles"
      "multivalued"                          = "true"
      "access.token.claim"                   = "true"
      "id.token.claim"                       = "false"
      "userinfo.token.claim"                 = "false"
    }
  }

  # alice holds nothing of her own: everything she may do comes from the modelling-team role.
  # The subject claim is `sub` (Identity__SubjectClaim), which Keycloak keeps stable across clients.
  realm_json = jsonencode({
    realm                      = local.realm
    enabled                    = true
    accessTokenLifespan        = 1800
    sslRequired                = "none"
    defaultDefaultClientScopes = []

    roles = {
      client = {
        (local.client_id) = [{ name = "modelling-team" }, { name = "supervisors" }]
      }
    }

    users = [
      {
        username      = "alice"
        enabled       = true
        email         = "alice@example.invalid"
        emailVerified = true
        firstName     = "Alice"
        lastName      = "Example"
        credentials   = [{ type = "password", value = "alice", temporary = false }]
        clientRoles   = { (local.client_id) = ["modelling-team"] }
      },
      {
        username      = "nobody"
        enabled       = true
        email         = "nobody@example.invalid"
        emailVerified = true
        firstName     = "No"
        lastName      = "Body"
        credentials   = [{ type = "password", value = "nobody", temporary = false }]
      },
    ]

    clients = [
      {
        clientId                  = local.client_id
        enabled                   = true
        protocol                  = "openid-connect"
        publicClient              = false
        secret                    = "armonik-secret"
        serviceAccountsEnabled    = true
        directAccessGrantsEnabled = true
        standardFlowEnabled       = false
        fullScopeAllowed          = true
        attributes                = { "use.refresh.tokens" = "false" }

        protocolMappers = [
          # Keycloak does not put the client in `aud` by default, and the control plane validates the audience
          {
            name           = "audience"
            protocol       = "openid-connect"
            protocolMapper = "oidc-audience-mapper"
            config = {
              "included.client.audience" = local.client_id
              "access.token.claim"       = "true"
            }
          },
          local.roles_mapper,
        ]
      },
      # Mints tokens for another audience. It carries no role of armonik-client, since Keycloak adds a client to
      # the audience of every token carrying its roles. Its tokens name itself as audience, so that they are refused
      # for an audience that is not the control plane's, not for carrying none
      {
        clientId                  = "other-client"
        enabled                   = true
        protocol                  = "openid-connect"
        publicClient              = false
        secret                    = "other-secret"
        directAccessGrantsEnabled = true
        standardFlowEnabled       = false
        fullScopeAllowed          = false
        defaultClientScopes       = []
        attributes                = { "use.refresh.tokens" = "false" }

        protocolMappers = [
          {
            name           = "audience"
            protocol       = "openid-connect"
            protocolMapper = "oidc-audience-mapper"
            config = {
              "included.client.audience" = "other-client"
              "access.token.claim"       = "true"
            }
          },
        ]
      },
    ]
  })
}

resource "docker_image" "keycloak" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "keycloak" {
  name  = local.container_name
  image = docker_image.keycloak.image_id

  networks_advanced {
    name = var.network.name
  }
  network_mode = var.network.driver

  log_driver = var.log_driver.name
  log_opts   = var.log_driver.log_opts

  # Test identity provider: no hostname or TLS setup, realm in memory
  command = ["start-dev", "--import-realm", "--health-enabled=true"]

  env = [
    "KC_BOOTSTRAP_ADMIN_USERNAME=admin",
    "KC_BOOTSTRAP_ADMIN_PASSWORD=admin",
    "KC_HTTP_ENABLED=true",
    "KC_HEALTH_ENABLED=true",
    # Pinned so that tokens requested from the host carry the issuer the control plane discovers in the network
    "KC_HOSTNAME=http://${local.container_name}:8080",
    "KC_HOSTNAME_STRICT=false",
  ]

  upload {
    file    = "/opt/keycloak/data/import/armonik-realm.json"
    content = local.realm_json
  }

  ports {
    internal = 8080
    external = var.exposed_port
  }

  # Wait for readiness so that tokens can be requested as soon as the deployment returns
  wait         = true
  wait_timeout = 180

  healthcheck {
    test = ["CMD-SHELL",
    "exec 3<>/dev/tcp/localhost/9000 && echo -e 'GET /health/ready HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' >&3 && cat <&3 | grep -q '\"status\": \"UP\"'"]
    interval     = "5s"
    retries      = 30
    timeout      = "5s"
    start_period = "10s"
  }
}
