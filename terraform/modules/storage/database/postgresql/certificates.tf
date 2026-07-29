#------------------------------------------------------------------------------
# Certificate Authority
#------------------------------------------------------------------------------
resource "tls_private_key" "root_postgresql" {
  count       = var.postgresql_params.ssl ? 1 : 0
  algorithm   = "RSA"
  ecdsa_curve = "P384"
  rsa_bits    = "4096"
}

resource "tls_self_signed_cert" "root_postgresql" {
  count                 = var.postgresql_params.ssl ? 1 : 0
  private_key_pem       = tls_private_key.root_postgresql[0].private_key_pem
  is_ca_certificate     = true
  validity_period_hours = 100000
  allowed_uses = [
    "cert_signing",
    "key_encipherment",
    "digital_signature"
  ]
  subject {
    organization = "ArmoniK postgresql Root (NonTrusted)"
    common_name  = "ArmoniK postgresql Root (NonTrusted) Private Certificate Authority"
    country      = "France"
  }
}

#------------------------------------------------------------------------------
# Certificate
#------------------------------------------------------------------------------
resource "tls_private_key" "postgresql_private_key" {
  count       = var.postgresql_params.ssl ? 1 : 0
  algorithm   = "RSA"
  ecdsa_curve = "P384"
  rsa_bits    = "4096"
}

resource "tls_cert_request" "postgresql_cert_request" {
  count           = var.postgresql_params.ssl ? 1 : 0
  private_key_pem = tls_private_key.postgresql_private_key[0].private_key_pem
  subject {
    country     = "France"
    common_name = "127.0.0.1"
  }
  ip_addresses = ["127.0.0.1"]
  dns_names    = [var.postgresql_params.database_name, "localhost"]
}

resource "tls_locally_signed_cert" "postgresql_certificate" {
  count                 = var.postgresql_params.ssl ? 1 : 0
  cert_request_pem      = tls_cert_request.postgresql_cert_request[0].cert_request_pem
  ca_private_key_pem    = tls_private_key.root_postgresql[0].private_key_pem
  ca_cert_pem           = tls_self_signed_cert.root_postgresql[0].cert_pem
  validity_period_hours = 100000
  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
    "client_auth",
    "any_extended",
  ]
}

resource "local_sensitive_file" "ca" {
  count           = var.postgresql_params.ssl ? 1 : 0
  content         = tls_self_signed_cert.root_postgresql[0].cert_pem
  filename        = "${path.root}/generated/postgresql/ca.pem"
  file_permission = "0644"
}
