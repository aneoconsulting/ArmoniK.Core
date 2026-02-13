FROM alpine:3.22.2 AS tini-build
RUN apk add gcc cmake make musl-dev
ADD https://github.com/krallin/tini.git .

# By default, tini uses buffered IO. Disable buffering to have logs in real-time
RUN sed -i '/int main/a setvbuf(stdout, NULL, _IOLBF, BUFSIZ);setvbuf(stderr, NULL, _IOLBF, BUFSIZ);' src/tini.c
RUN cmake . && make

FROM mcr.microsoft.com/dotnet/aspnet:10.0-azurelinux3.0-distroless AS base-linux
COPY --from=tini-build /tini-static /tini
USER $APP_UID
ENTRYPOINT [ "/tini", "-s", "-vv", "--", "dotnet" ]

FROM mcr.microsoft.com/dotnet/aspnet:10.0-nanoserver-ltsc2022 AS base-windows
ENTRYPOINT ["C:\\Program Files\\dotnet\\dotnet.exe"]

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build-linux
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0-nanoserver-ltsc2022 AS build-windows

# Configure sh for RUN commands
RUN pwsh -Command "Invoke-WebRequest -Uri https://frippery.org/files/busybox/busybox64u.exe -OutFile 'C:\\Program Files\\busybox.exe'"
RUN pwsh -Command "Set-Content -Path 'C:\\Program Files\\sh.ps1' -Value '$wrapper = ''\"C:\Program Files\sh.ps1\"''; $cmdLine = [Environment]::CommandLine; $idx = $cmdLine.IndexOf($wrapper) + $wrapper.Length; $cmd = $cmdLine.Substring($idx).TrimStart(); $cmd | & ''C:\Program Files\busybox.exe'' sh'"
SHELL ["pwsh", "-File", "\"C:\\Program Files\\sh.ps1\""]

FROM build-${BUILDOS} AS build
ARG VERSION=1.0.0.0
ARG TARGETARCH
ARG TARGETOS

WORKDIR /src
# git ls-tree -r HEAD --name-only --full-tree | grep "csproj$" | xargs -I % sh -c "export D=\$(dirname %) ; echo COPY [\\\"%\\\", \\\"\$D/\\\"]"
COPY ["Adaptors/Amqp/src/ArmoniK.Core.Adapters.Amqp.csproj", "Adaptors/Amqp/src/"]
COPY ["Adaptors/LocalStorage/src/ArmoniK.Core.Adapters.LocalStorage.csproj", "Adaptors/LocalStorage/src/"]
COPY ["Adaptors/Memory/src/ArmoniK.Core.Adapters.Memory.csproj", "Adaptors/Memory/src/"]
COPY ["Adaptors/MongoDB/src/ArmoniK.Core.Adapters.MongoDB.csproj", "Adaptors/MongoDB/src/"]
COPY ["Adaptors/QueueCommon/src/ArmoniK.Core.Adapters.QueueCommon.csproj", "Adaptors/QueueCommon/src/"]
COPY ["Adaptors/PubSub/src/ArmoniK.Core.Adapters.PubSub.csproj", "Adaptors/PubSub/src/"]
COPY ["Adaptors/Nats/src/ArmoniK.Core.Adapters.Nats.csproj", "Adaptors/Nats/src/"]
COPY ["Adaptors/Redis/src/ArmoniK.Core.Adapters.Redis.csproj", "Adaptors/Redis/src/"]
COPY ["Adaptors/S3/src/ArmoniK.Core.Adapters.S3.csproj", "Adaptors/S3/src/"]
COPY ["Adaptors/SQS/src/ArmoniK.Core.Adapters.SQS.csproj", "Adaptors/SQS/src/"]
COPY ["Adaptors/Embed/src/ArmoniK.Core.Adapters.Embed.csproj", "Adaptors/Embed/src/"]
COPY ["Adaptors/NullStorage/src/ArmoniK.Core.Adapters.NullStorage.csproj", "Adaptors/NullStorage/src/"]
COPY ["Base/src/ArmoniK.Core.Base.csproj", "Base/src/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
COPY ["Compute/PollingAgent/src/ArmoniK.Core.Compute.PollingAgent.csproj", "Compute/PollingAgent/src/"]
COPY ["Control/Metrics/src/ArmoniK.Core.Control.Metrics.csproj", "Control/Metrics/src/"]
COPY ["Control/PartitionMetrics/src/ArmoniK.Core.Control.PartitionMetrics.csproj", "Control/PartitionMetrics/src/"]
COPY ["Control/Submitter/src/ArmoniK.Core.Control.Submitter.csproj", "Control/Submitter/src/"]
COPY ["Utils/src/ArmoniK.Core.Utils.csproj", "Utils/src/"]

RUN dotnet restore -a "${TARGETARCH}" "Compute/PollingAgent/src/ArmoniK.Core.Compute.PollingAgent.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Control/Metrics/src/ArmoniK.Core.Control.Metrics.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Control/PartitionMetrics/src/ArmoniK.Core.Control.PartitionMetrics.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Control/Submitter/src/ArmoniK.Core.Control.Submitter.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Adaptors/Amqp/src/ArmoniK.Core.Adapters.Amqp.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Adaptors/PubSub/src/ArmoniK.Core.Adapters.PubSub.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Adaptors/Nats/src/ArmoniK.Core.Adapters.Nats.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Adaptors/SQS/src/ArmoniK.Core.Adapters.SQS.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Adaptors/S3/src/ArmoniK.Core.Adapters.S3.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Adaptors/LocalStorage/src/ArmoniK.Core.Adapters.LocalStorage.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Adaptors/Redis/src/ArmoniK.Core.Adapters.Redis.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Adaptors/Embed/src/ArmoniK.Core.Adapters.Embed.csproj"
RUN dotnet restore -a "${TARGETARCH}" "Adaptors/NullStorage/src/ArmoniK.Core.Adapters.NullStorage.csproj"

# git ls-tree -r HEAD --name-only --full-tree | grep "csproj$" | xargs -I % sh -c "export D=\$(dirname %) ; echo COPY [\\\"\$D\\\", \\\"\$D\\\"]"
COPY ["Adaptors/Amqp/src", "Adaptors/Amqp/src"]
COPY ["Adaptors/LocalStorage/src", "Adaptors/LocalStorage/src"]
COPY ["Adaptors/Memory/src", "Adaptors/Memory/src"]
COPY ["Adaptors/MongoDB/src", "Adaptors/MongoDB/src"]
COPY ["Adaptors/QueueCommon/src", "Adaptors/QueueCommon/src"]
COPY ["Adaptors/PubSub/src", "Adaptors/PubSub/src"]
COPY ["Adaptors/Nats/src", "Adaptors/Nats/src"]
COPY ["Adaptors/Redis/src", "Adaptors/Redis/src"]
COPY ["Adaptors/S3/src", "Adaptors/S3/src"]
COPY ["Adaptors/SQS/src", "Adaptors/SQS/src"]
COPY ["Adaptors/Embed/src", "Adaptors/Embed/src"]
COPY ["Adaptors/NullStorage/src", "Adaptors/NullStorage/src"]
COPY ["Base/src", "Base/src"]
COPY ["Common/src", "Common/src"]
COPY ["Compute/PollingAgent/src", "Compute/PollingAgent/src"]
COPY ["Control/Metrics/src", "Control/Metrics/src"]
COPY ["Control/PartitionMetrics/src", "Control/PartitionMetrics/src"]
COPY ["Control/Submitter/src", "Control/Submitter/src"]
COPY ["Utils/src", "Utils/src"]

WORKDIR /src/Adaptors/SQS/src
RUN dotnet publish "ArmoniK.Core.Adapters.SQS.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/sqs /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Adaptors/PubSub/src
RUN dotnet publish "ArmoniK.Core.Adapters.PubSub.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/pubsub /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Adaptors/Nats/src
RUN dotnet publish "ArmoniK.Core.Adapters.Nats.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/nats /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Adaptors/Amqp/src
RUN dotnet publish "ArmoniK.Core.Adapters.Amqp.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/amqp /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Adaptors/LocalStorage/src
RUN dotnet publish "ArmoniK.Core.Adapters.LocalStorage.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/local_storage /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Adaptors/Embed/src
RUN dotnet publish "ArmoniK.Core.Adapters.Embed.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/embed /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Adaptors/NullStorage/src
RUN dotnet publish "ArmoniK.Core.Adapters.NullStorage.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/null_storage /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Adaptors/Redis/src
RUN dotnet publish "ArmoniK.Core.Adapters.Redis.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/redis /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Adaptors/S3/src
RUN dotnet publish "ArmoniK.Core.Adapters.S3.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/s3 /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Compute/PollingAgent/src
RUN dotnet publish "ArmoniK.Core.Compute.PollingAgent.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/polling_agent /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Control/Metrics/src
RUN dotnet publish "ArmoniK.Core.Control.Metrics.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/metrics /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Control/PartitionMetrics/src
RUN dotnet publish "ArmoniK.Core.Control.PartitionMetrics.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/partition_metrics /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION

WORKDIR /src/Control/Submitter/src
RUN dotnet publish "ArmoniK.Core.Control.Submitter.csproj" -a "${TARGETARCH}" --no-restore -o /app/publish/submitter /p:UseAppHost=false -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion=$VERSION -p:Version=$VERSION


FROM base-${TARGETOS} AS polling_agent
WORKDIR /adapters/queue/sqs
COPY --from=build /app/publish/sqs .
WORKDIR /adapters/queue/pubsub
COPY --from=build /app/publish/pubsub .
WORKDIR /adapters/queue/nats
COPY --from=build /app/publish/nats .
WORKDIR /adapters/queue/amqp
COPY --from=build /app/publish/amqp .
WORKDIR /adapters/object/local_storage
COPY --from=build /app/publish/local_storage .
WORKDIR /adapters/object/redis
COPY --from=build /app/publish/redis .
WORKDIR /adapters/object/embed
COPY --from=build /app/publish/embed .
WORKDIR /adapters/object/null_storage
COPY --from=build /app/publish/null_storage .
WORKDIR /adapters/object/s3
COPY --from=build /app/publish/s3 .
WORKDIR /app
COPY --from=build /app/publish/polling_agent .
ENV ASPNETCORE_URLS http://+:1080
EXPOSE 1080
CMD ["ArmoniK.Core.Compute.PollingAgent.dll"]


FROM base-${TARGETOS} AS metrics
WORKDIR /app
COPY --from=build /app/publish/metrics .
ENV ASPNETCORE_URLS http://+:1080
EXPOSE 1080
CMD ["ArmoniK.Core.Control.Metrics.dll"]


FROM base-${TARGETOS} AS partition_metrics
WORKDIR /app
COPY --from=build /app/publish/partition_metrics .
ENV ASPNETCORE_URLS http://+:1080
EXPOSE 1080
CMD ["ArmoniK.Core.Control.PartitionMetrics.dll"]


FROM base-${TARGETOS} AS submitter
WORKDIR /adapters/queue/sqs
COPY --from=build /app/publish/sqs .
WORKDIR /adapters/queue/pubsub
COPY --from=build /app/publish/pubsub .
WORKDIR /adapters/queue/nats
COPY --from=build /app/publish/nats .
WORKDIR /adapters/queue/amqp
COPY --from=build /app/publish/amqp .
WORKDIR /adapters/object/local_storage
COPY --from=build /app/publish/local_storage .
WORKDIR /adapters/object/redis
COPY --from=build /app/publish/redis .
WORKDIR /adapters/object/embed
COPY --from=build /app/publish/embed .
WORKDIR /adapters/object/null_storage
COPY --from=build /app/publish/null_storage .
WORKDIR /adapters/object/s3
COPY --from=build /app/publish/s3 .
WORKDIR /app
COPY --from=build /app/publish/submitter .
ENV ASPNETCORE_URLS http://+:1080, http://+:1081
EXPOSE 1080
EXPOSE 1081
CMD ["ArmoniK.Core.Control.Submitter.dll"]
