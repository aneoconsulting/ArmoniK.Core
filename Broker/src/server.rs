// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Accept loop: HTTP/1.1 and HTTP/2 (h2 over TLS through ALPN, h2c in clear),
//! optional TLS and mutual TLS, TCP keepalive and HTTP/2 PING frames (design C.4).

use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use rustls::ServerConfig;
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::actor::Registry;
use crate::config::Config;

fn tls_config(cfg: &Config) -> anyhow::Result<Option<Arc<ServerConfig>>> {
    let (Some(cert), Some(key)) = (&cfg.tls_cert, &cfg.tls_key) else {
        return Ok(None);
    };
    let certs: Vec<CertificateDer> =
        CertificateDer::pem_reader_iter(&mut BufReader::new(std::fs::File::open(cert)?))
            .collect::<Result<_, _>>()?;
    let key = PrivateKeyDer::from_pem_reader(&mut BufReader::new(std::fs::File::open(key)?))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let builder = match &cfg.tls_client_ca {
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            for c in CertificateDer::pem_reader_iter(&mut BufReader::new(std::fs::File::open(ca)?))
            {
                roots.add(c?)?;
            }
            let verifier =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider).build()?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut sc = builder.with_single_cert(certs, key)?;
    sc.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Some(Arc::new(sc)))
}

pub async fn serve(
    reg: Arc<Registry>,
    shutdown: impl std::future::Future<Output = ()>,
) -> anyhow::Result<()> {
    let cfg = reg.cfg.clone();
    let tls = tls_config(&cfg)?.map(TlsAcceptor::from);
    let listener = TcpListener::bind(cfg.listen).await?;
    tracing::info!(listen = %cfg.listen, tls = tls.is_some(), mtls = cfg.tls_client_ca.is_some(), epoch = reg.epoch, "broker listening");

    let app = crate::http::app(reg.clone());
    let ping = Duration::from_millis(cfg.ping_ms);
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(Some(ping))
        .keep_alive_timeout(ping);
    builder.http1().timer(TokioTimer::new());
    let builder = Arc::new(builder);

    tokio::pin!(shutdown);
    loop {
        let (stream, peer) = tokio::select! {
            r = listener.accept() => match r {
                Ok(x) => x,
                Err(e) => { tracing::warn!(error = %e, "accept failed"); continue; }
            },
            _ = &mut shutdown => break,
        };
        let sock = socket2::SockRef::from(&stream);
        let _ = sock.set_tcp_keepalive(
            &socket2::TcpKeepalive::new()
                .with_time(ping)
                .with_interval(ping),
        );
        let _ = stream.set_nodelay(true);
        let service = TowerToHyperService::new(app.clone());
        let builder = builder.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let result = match tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(s) => builder.serve_connection(TokioIo::new(s), service).await,
                    Err(e) => {
                        tracing::debug!(%peer, error = %e, "TLS handshake failed");
                        return;
                    }
                },
                None => {
                    builder
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                }
            };
            if let Err(e) = result {
                tracing::debug!(%peer, error = %e, "connection closed with error");
            }
        });
    }
    // Clean stop: sleepers get an empty answer, new requests get 503 shutting-down.
    reg.shutdown();
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}
