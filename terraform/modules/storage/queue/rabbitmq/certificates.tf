resource "tls_private_key" "rabbit_private_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "tls_self_signed_cert" "rabbit_ca" {
  private_key_pem       = tls_private_key.rabbit_private_key.private_key_pem
  is_ca_certificate     = true
  validity_period_hours = 87600
  allowed_uses = [
    "cert_signing",
    "key_encipherment",
    "digital_signature"
  ]
  subject {
    organization = "ArmoniK RabbitMQ Root CA"
    common_name  = "ArmoniK Root Certificate Authority"
    country      = "FR"
  }
}

resource "tls_cert_request" "rabbit_cert_request" {
  private_key_pem = tls_private_key.rabbit_private_key.private_key_pem
  subject {
    country      = "FR"
    organization = "ArmoniK"
    common_name  = "queue"
  }
  ip_addresses = ["127.0.0.1"]
  dns_names    = ["localhost", "queue"]
}

resource "tls_locally_signed_cert" "rabbit_cert" {
  cert_request_pem      = tls_cert_request.rabbit_cert_request.cert_request_pem
  ca_private_key_pem    = tls_private_key.rabbit_private_key.private_key_pem
  ca_cert_pem           = tls_self_signed_cert.rabbit_ca.cert_pem
  validity_period_hours = 87600
  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
    "client_auth"
  ]
}

resource "local_file" "ca" {
  content         = tls_self_signed_cert.rabbit_ca.cert_pem
  filename        = "${path.root}/generated/queue/certs/ca.pem"
  file_permission = "0644"
}

resource "local_file" "cert" {
  content         = tls_locally_signed_cert.rabbit_cert.cert_pem
  filename        = "${path.root}/generated/queue/certs/rabbit.crt"
  file_permission = "0644"
}

resource "local_file" "key" {
  content         = tls_private_key.rabbit_private_key.private_key_pem
  filename        = "${path.root}/generated/queue/certs/rabbit.key"
  file_permission = "0600"
}