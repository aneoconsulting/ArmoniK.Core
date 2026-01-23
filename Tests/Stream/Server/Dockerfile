#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base-linux
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
RUN mkdir /cache && chown armonikuser: /cache
USER armonikuser
ENV ASPNETCORE_URLS http://+:1080;https://+1443
EXPOSE 1080
EXPOSE 1443
ENTRYPOINT [ "dotnet" ]

FROM mcr.microsoft.com/dotnet/sdk:8.0-nanoserver-ltsc2022 AS base-windows
ENTRYPOINT ["C:\\Program Files\\dotnet\\dotnet.exe"]

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build-linux
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0-nanoserver-ltsc2022 AS build-windows

# Configure sh for RUN commands
RUN pwsh -Command "Invoke-WebRequest -Uri https://frippery.org/files/busybox/busybox64u.exe -OutFile 'C:\\Program Files\\busybox.exe'"
RUN pwsh -Command "Set-Content -Path 'C:\\Program Files\\sh.ps1' -Value '$wrapper = ''\"C:\Program Files\sh.ps1\"''; $cmdLine = [Environment]::CommandLine; $idx = $cmdLine.IndexOf($wrapper) + $wrapper.Length; $cmd = $cmdLine.Substring($idx).TrimStart(); $cmd | & ''C:\Program Files\busybox.exe'' sh'"
SHELL ["pwsh", "-File", "\"C:\\Program Files\\sh.ps1\""]

FROM build-${BUILDOS} AS build
ARG TARGETARCH
ARG TARGETOS
ARG VERSION=1.0.0.0

WORKDIR /src
COPY ["Tests/Stream/Server/ArmoniK.Extensions.Common.StreamWrapper.Tests.Server.csproj", "Tests/Stream/Server/"]
COPY ["Tests/Stream/Common/ArmoniK.Extensions.Common.StreamWrapper.Tests.Common.csproj", "Tests/Stream/Common/"]
RUN dotnet restore -a "${TARGETARCH}" "Tests/Stream/Server/ArmoniK.Extensions.Common.StreamWrapper.Tests.Server.csproj"
COPY ["Tests/Stream/Server", "Tests/Stream/Server"]
COPY ["Tests/Stream/Common", "Tests/Stream/Common"]
WORKDIR "/src/Tests/Stream/Server"
RUN dotnet publish -a "${TARGETARCH}" "ArmoniK.Extensions.Common.StreamWrapper.Tests.Server.csproj" --no-restore -o /app/publish -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion="$VERSION" -p:Version="$VERSION"

FROM base-${TARGETOS} AS final
WORKDIR /app
COPY --from=build /app/publish .
CMD ["ArmoniK.Extensions.Common.StreamWrapper.Tests.Server.dll"]
