resource "tls_private_key" "redis_private_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "tls_self_signed_cert" "redis_ca" {
  private_key_pem       = tls_private_key.redis_private_key.private_key_pem
  is_ca_certificate     = true
  validity_period_hours = 87600
  allowed_uses = [
    "cert_signing",
    "key_encipherment",
    "digital_signature"
  ]
  subject {
    organization = "ArmoniK Redis Root CA"
    common_name  = "ArmoniK Root Certificate Authority"
    country      = "FR"
  }
}

resource "tls_cert_request" "redis_cert_request" {
  private_key_pem = tls_private_key.redis_private_key.private_key_pem
  subject {
    country      = "FR"
    organization = "ArmoniK"
    common_name  = "redis"
  }
  ip_addresses = ["127.0.0.1", "172.20.0.6"]
  dns_names    = ["localhost", "redis"]
}

resource "tls_locally_signed_cert" "redis_cert" {
  cert_request_pem      = tls_cert_request.redis_cert_request.cert_request_pem
  ca_private_key_pem    = tls_private_key.redis_private_key.private_key_pem
  ca_cert_pem           = tls_self_signed_cert.redis_ca.cert_pem
  validity_period_hours = 87600
  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
    "client_auth"
  ]
}

resource "local_file" "ca" {
  content         = tls_self_signed_cert.redis_ca.cert_pem
  filename        = "${path.root}/generated/redis/certs/ca.pem"
  file_permission = "0644"
}

resource "local_file" "cert" {
  content         = tls_locally_signed_cert.redis_cert.cert_pem
  filename        = "${path.root}/generated/redis/certs/redis.crt"
  file_permission = "0644"
}

resource "local_file" "key" {
  content         = tls_private_key.redis_private_key.private_key_pem
  filename        = "${path.root}/generated/redis/certs/redis.key"
  file_permission = "0600"
}