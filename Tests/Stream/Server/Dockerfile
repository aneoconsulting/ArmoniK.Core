#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:10.0-azurelinux3.0-distroless AS base-linux
USER $APP_UID
ENTRYPOINT [ "dotnet" ]

FROM mcr.microsoft.com/dotnet/sdk:10.0-nanoserver-ltsc2022 AS base-windows
ENTRYPOINT ["C:\\Program Files\\dotnet\\dotnet.exe"]

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build
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
