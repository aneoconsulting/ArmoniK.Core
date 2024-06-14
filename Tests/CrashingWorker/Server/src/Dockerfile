#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base-linux
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
RUN mkdir /cache && chown armonikuser: /cache
USER armonikuser

FROM mcr.microsoft.com/dotnet/runtime:8.0-nanoserver-ltsc2022 AS base-windows

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG TARGETARCH
ARG TARGETOS
ARG VERSION=1.0.0.0

WORKDIR /src
COPY ["Tests/CrashingWorker/Server/src/ArmoniK.Samples.CrashingWorker.Server.csproj", "Tests/CrashingWorker/Server/src/"]
RUN dotnet restore -a "${TARGETARCH}" "Tests/CrashingWorker/Server/src/ArmoniK.Samples.CrashingWorker.Server.csproj"
COPY ["Tests/CrashingWorker/Server/src", "Tests/CrashingWorker/Server/src"]
WORKDIR "/src/Tests/CrashingWorker/Server/src"
RUN dotnet publish -a "${TARGETARCH}" "ArmoniK.Samples.CrashingWorker.Server.csproj" -o /app/publish -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion="$VERSION" -p:Version="$VERSION"

FROM base-${TARGETOS} AS final
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 1080
ENTRYPOINT ["dotnet", "ArmoniK.Samples.CrashingWorker.Server.dll"]
