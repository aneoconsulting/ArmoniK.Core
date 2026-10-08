# Envvars
locals {
  armonik_conf = <<EOF

map $http_upgrade $connection_upgrade {
    default upgrade;
    '' close;
}

%{if var.mtls~}
    map $ssl_client_s_dn $ssl_client_s_dn_cn {
        default "";
        ~CN=(?<CN>[^,/]+) $CN;
    }
%{endif~}
server {
%{if var.tls~}
    listen 8443 ssl http2;
    listen [::]:8443 ssl http2;
    listen 9443 ssl http2;
    listen [::]:9443 ssl http2;
    ssl_certificate     /ingress.crt;
    ssl_certificate_key /ingress.key;
%{if var.mtls~}
    ssl_verify_client on;
    ssl_client_certificate /client_ca.crt;
%{else~}
    ssl_verify_client off;
%{endif~}
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers EECDH+AESGCM:EECDH+AES256;
    ssl_conf_command Ciphersuites TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256;
%{else~}
    listen 8080;
    listen [::]:8080;
    listen 9080 http2;
    listen [::]:9080 http2;
%{endif~}

    sendfile on;

    # Docker embedded DNS, used by the grpc_pass variable below so that the submitter address is
    # resolved at runtime instead of once at startup (a restarted submitter may change address)
    resolver 127.0.0.11 valid=10s ipv6=off;

    location ~* ^/armonik\. {
%{if var.mtls~}
        grpc_set_header X-Certificate-Client-CN $ssl_client_s_dn_cn;
        grpc_set_header X-Certificate-Client-Fingerprint $ssl_client_fingerprint;
        # Full client certificate (URL-escaped PEM), nginx exposes no variable for the organizational units
        grpc_set_header X-Forwarded-Client-Cert $ssl_client_escaped_cert;
%{else~}
        # No client certificate is verified on this listener: drop any identity header sent by the client
        # (proxy_hide_header only applies to response headers)
        grpc_set_header X-Certificate-Client-CN "";
        grpc_set_header X-Certificate-Client-Fingerprint "";
        grpc_set_header X-Forwarded-Client-Cert "";
%{endif~}
        # Through a variable so that the resolver above is used
        set $submitter_upstream grpc://${var.submitter.url}:${var.submitter.port};
        grpc_pass $submitter_upstream;

        # Apparently, multiple chunks in a grpc stream is counted has a single body
        # So disable the limit
        client_max_body_size 0;

        # add a timeout of 1 month to avoid grpc exception for long task
        # TODO: find better configuration
        proxy_read_timeout 30d;
        proxy_send_timeout 1d;
        grpc_read_timeout 30d;
        grpc_send_timeout 1d;
    }
}
EOF
}
