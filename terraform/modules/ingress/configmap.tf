# Envvars
locals {
  armonik_conf = <<EOF

map $http_upgrade $connection_upgrade {
    default upgrade;
    '' close;
}
%{if var.proxy != null~}

# Content type of a trailers-only gRPC error, mirroring the flavor of the request
map $http_content_type $grpc_error_content_type {
    default application/grpc;
    ~*^application/grpc-web-text application/grpc-web-text+proto;
    ~*^application/grpc-web application/grpc-web+proto;
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

%{if var.proxy != null~}
    # The authentication service validates the credential of the caller, here a token, and answers its user and
    # groups, as oauth2-proxy does behind auth_request (--set-xauthrequest)
    location = /oauth2/auth {
        internal;
        set $auth_upstream http://${var.proxy.url};
        proxy_pass $auth_upstream;
        proxy_pass_request_body off;
        proxy_set_header Content-Length "";
        proxy_set_header Host $host;
        proxy_set_header X-Original-URI $request_uri;
        # X-Auth-Request-Groups may exceed the 4 KB default
        proxy_buffer_size 64k;
        proxy_buffers 8 64k;
    }

    # Trailers-only gRPC errors, so that the client reports the status instead of an HTTP error
    location @grpc_unauthenticated {
        default_type "";
        add_header Content-Type $grpc_error_content_type always;
        add_header grpc-status 16 always;
        add_header grpc-message "Unauthenticated" always;
        return 200;
    }
    location @grpc_permission_denied {
        default_type "";
        add_header Content-Type $grpc_error_content_type always;
        add_header grpc-status 7 always;
        add_header grpc-message "Permission denied" always;
        return 200;
    }
%{endif~}

    location ~* ^/armonik\. {
%{if var.proxy != null~}
        auth_request /oauth2/auth;
        error_page 401 = @grpc_unauthenticated;
        error_page 403 = @grpc_permission_denied;
        # One line, the groups joined by commas, as the Proxy method reads it
        auth_request_set $proxy_user $upstream_http_x_auth_request_user;
        auth_request_set $proxy_groups $upstream_http_x_auth_request_groups;
        # The identity of the authentication service only, never one sent by the client. The credential it
        # validated is dropped: the control plane refuses a request presenting two
        grpc_set_header X-Forwarded-User $proxy_user;
        grpc_set_header X-Forwarded-Groups $proxy_groups;
        grpc_set_header X-Forwarded-Client-Cert "";
        grpc_set_header Authorization "";
%{else~}
%{if var.mtls~}
        # Client certificate in the Envoy XFCC format, replacing any header sent by the client. No Hash:
        # $ssl_client_fingerprint is SHA-1 where Envoy writes SHA-256
        grpc_set_header X-Forwarded-Client-Cert "Cert=\"$ssl_client_escaped_cert\"";
%{else~}
        # No client certificate is verified on this listener: drop the identity header sent by the client
        # (proxy_hide_header only applies to response headers)
        grpc_set_header X-Forwarded-Client-Cert "";
%{endif~}
        # No authentication service on this listener: drop the user and groups sent by the client
        grpc_set_header X-Forwarded-User "";
        grpc_set_header X-Forwarded-Groups "";
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
