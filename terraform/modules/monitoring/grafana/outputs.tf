output "grafana_env_vars" {
  value = ({
    "GF_AUTH_ANONYMOUS_ENABLED"  = true
    "GF_AUTH_ANONYMOUS_ORG_ROLE" = "Admin"
    "GF_AUTH_DISABLE_LOGIN_FORM" = true
  })
}