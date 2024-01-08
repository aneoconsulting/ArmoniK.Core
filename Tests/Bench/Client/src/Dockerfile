#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG TARGETARCH
ARG VERSION=1.0.0.0

WORKDIR /src
COPY ["Tests/Bench/Client/src/ArmoniK.Samples.Bench.Client.csproj", "Tests/Bench/Client/src/"]
COPY ["Tests/Common/Client/src/ArmoniK.Core.Common.Tests.Client.csproj", "Tests/Common/Client/src/"]
COPY ["Utils/src/ArmoniK.Core.Utils.csproj", "Utils/src/"]
COPY ["Base/src/ArmoniK.Core.Base.csproj", "Base/src/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
RUN dotnet restore -a "${TARGETARCH}" "Tests/Bench/Client/src/ArmoniK.Samples.Bench.Client.csproj"
COPY ["Tests/Bench/Client/src", "Tests/Bench/Client/src"]
COPY ["Tests/Common/Client/src", "Tests/Common/Client/src"]
COPY ["Utils/src/", "Utils/src/"]
COPY ["Base/src/", "Base/src/"]
COPY ["Common/src", "Common/src"]
WORKDIR "/src/Tests/Bench/Client/src"
RUN dotnet publish -a "${TARGETARCH}" "ArmoniK.Samples.Bench.Client.csproj" --no-restore -o /app/publish -p:RunAnalyzers=false -p:WarningLevel=0 -p:PackageVersion="$VERSION" -p:Version="$VERSION"

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser

USER armonikuser
ENTRYPOINT ["dotnet", "ArmoniK.Samples.Bench.Client.dll"]
