// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! ArmoniK Broker binary; the queue itself lives in the library (lib.rs).

use armonik_broker::{actor, config, server};

use clap::Parser;

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
    tracing::info!("shutdown requested");
}

/// `armonik-broker health`: container health probe without extra tooling.
fn health_probe() -> ! {
    use std::io::{Read, Write};
    let port = std::env::var("BROKER_LISTEN")
        .ok()
        .and_then(|l| l.rsplit(':').next().map(str::to_string))
        .unwrap_or_else(|| "8080".into());
    let ok = std::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .and_then(|mut s| {
            s.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
            s.write_all(
                b"GET /v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )?;
            let mut buf = String::new();
            s.read_to_string(&mut buf)?;
            Ok(buf.starts_with("HTTP/1.1 200"))
        })
        .unwrap_or(false);
    std::process::exit(if ok { 0 } else { 1 })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("health") {
        health_probe();
    }
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("BROKER_LOG")
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cfg = config::Config::parse();
    let reg = actor::Registry::new(cfg);
    server::serve(reg, shutdown_signal()).await
}
