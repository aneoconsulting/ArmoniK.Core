locals {
  server_files = merge({
    "/etc/nginx/conf.d/default.conf" = local.armonik_conf
    },
    var.tls ? { "/ingress.crt" = tls_locally_signed_cert.ingress_certificate.0.cert_pem, "/ingress.key" = tls_private_key.ingress_private_key.0.private_key_pem } : {},
  var.mtls ? { "/client_ca.crt" = tls_self_signed_cert.client_root_ingress.0.cert_pem } : {})
}