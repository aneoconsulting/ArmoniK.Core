#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:10.0-azurelinux3.0-distroless AS base-linux
USER $APP_UID
ENTRYPOINT [ "dotnet" ]

FROM mcr.microsoft.com/dotnet/aspnet:10.0-nanoserver-ltsc2022 AS base-windows
ENTRYPOINT ["C:\\Program Files\\dotnet\\dotnet.exe"]

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG TARGETARCH
ARG TARGETOS
ARG VERSION=1.0.0.0

WORKDIR /src
COPY ["Tests/Bench/Server/src/ArmoniK.Samples.Bench.Server.csproj", "Tests/Bench/Server/src/"]
RUN dotnet restore -a "${TARGETARCH}" "Tests/Bench/Server/src/ArmoniK.Samples.Bench.Server.csproj"
COPY ["Tests/Bench/Server/src", "Tests/Bench/Server/src"]
WORKDIR "/src/Tests/Bench/Server/src"
RUN dotnet publish -a "${TARGETARCH}" "ArmoniK.Samples.Bench.Server.csproj" -o /app/publish -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion="$VERSION" -p:Version="$VERSION"

FROM base-${TARGETOS} AS final
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 1080
CMD ["ArmoniK.Samples.Bench.Server.dll"]
