#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:8.0-jammy-chiseled AS base-linux
USER $APP_UID
ENTRYPOINT [ "dotnet" ]

FROM mcr.microsoft.com/dotnet/aspnet:8.0-windowsservercore-ltsc2022 AS base-windows
ENTRYPOINT ["C:\\Program Files\\dotnet\\dotnet.exe"]

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG TARGETARCH
ARG TARGETOS
ARG VERSION=1.0.0.0

WORKDIR /src
COPY ["Tests/HtcMock/Client/src/ArmoniK.Samples.HtcMock.Client.csproj", "Tests/HtcMock/Client/src/"]
COPY ["Tests/Common/Client/src/ArmoniK.Core.Common.Client.csproj", "Tests/Common/Client/src/"]
RUN dotnet restore -a "${TARGETARCH}" "Tests/HtcMock/Client/src/ArmoniK.Samples.HtcMock.Client.csproj"
COPY ["Tests/HtcMock/Client/src", "Tests/HtcMock/Client/src"]
COPY ["Tests/Common/Client/src", "Tests/Common/Client/src"]
WORKDIR "/src/Tests/HtcMock/Client/src"
RUN dotnet publish -a "${TARGETARCH}" "ArmoniK.Samples.HtcMock.Client.csproj" --no-restore -o /app/publish -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion="$VERSION" -p:Version="$VERSION"

FROM base-${TARGETOS} AS final
WORKDIR /app
COPY --from=build /app/publish .
CMD ["ArmoniK.Samples.HtcMock.Client.dll"]
