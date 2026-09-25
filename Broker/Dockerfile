# ArmoniK Broker image. Build context: repository root (just buildBroker).
# Cross-compiled on the build platform for linux/amd64, linux/arm64 and windows/amd64,
# like the other Core images; the final stages only copy the binary.

FROM --platform=$BUILDPLATFORM rust:1-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
RUN set -eux; \
    case "${TARGETOS}/${TARGETARCH}" in \
      linux/amd64)   target=x86_64-unknown-linux-gnu;  cc=x86_64-linux-gnu-gcc;  pkgs="gcc-x86-64-linux-gnu libc6-dev-amd64-cross" ;; \
      linux/arm64)   target=aarch64-unknown-linux-gnu; cc=aarch64-linux-gnu-gcc; pkgs="gcc-aarch64-linux-gnu libc6-dev-arm64-cross" ;; \
      windows/amd64) target=x86_64-pc-windows-gnu;     cc=x86_64-w64-mingw32-gcc; pkgs="gcc-mingw-w64-x86-64" ;; \
      *) echo "unsupported platform ${TARGETOS}/${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    if ! command -v "$cc"; then \
      apt-get update; apt-get install -y --no-install-recommends $pkgs; rm -rf /var/lib/apt/lists/*; \
    fi; \
    rustup target add "$target"; \
    echo "$target" > /target; echo "$cc" > /cc
WORKDIR /src
COPY Broker/Cargo.toml Broker/Cargo.lock ./
COPY Broker/src src
COPY Broker/benches benches
COPY Broker/conformance conformance
RUN set -eux; \
    target="$(cat /target)"; cc="$(cat /cc)"; \
    export "CC_$(echo "$target" | tr - _)=$cc"; \
    export "CARGO_TARGET_$(echo "$target" | tr a-z- A-Z_)_LINKER=$cc"; \
    cargo build --release --locked --target "$target"; \
    mkdir /out; \
    cp "target/$target/release/armonik-broker" /out/ 2>/dev/null || cp "target/$target/release/armonik-broker.exe" /out/

FROM gcr.io/distroless/cc-debian12:nonroot AS base-linux
COPY --from=build /out/armonik-broker /usr/local/bin/armonik-broker
ENV BROKER_LISTEN=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/armonik-broker"]

FROM mcr.microsoft.com/windows/nanoserver:ltsc2022 AS base-windows
COPY --from=build /out/armonik-broker.exe C:/armonik/armonik-broker.exe
ENV BROKER_LISTEN=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["C:\\armonik\\armonik-broker.exe"]

FROM base-${TARGETOS}
