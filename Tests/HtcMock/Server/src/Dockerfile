#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["Tests/HtcMock/Server/src/ArmoniK.Samples.HtcMock.Server.csproj", "Tests/HtcMock/Server/src/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
RUN dotnet restore "Tests/HtcMock/Server/src/ArmoniK.Samples.HtcMock.Server.csproj"
COPY ["Tests/HtcMock/Server/src", "Tests/HtcMock/Server/src"]
COPY ["Common/src", "Common/src"]
WORKDIR "/src/Tests/HtcMock/Server/src"
RUN dotnet build "ArmoniK.Samples.HtcMock.Server.csproj" -c Release -o /app/build -p:RunAnalyzers=false -p:WarningLevel=0

FROM build AS publish
ARG VERSION=1.0.0.0
RUN dotnet publish "ArmoniK.Samples.HtcMock.Server.csproj" -c Release -o /app/publish -p:PackageVersion=$VERSION -p:Version=$VERSION

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
RUN mkdir /cache && chown armonikuser: /cache
USER armonikuser
EXPOSE 1080
ENTRYPOINT ["dotnet", "ArmoniK.Samples.HtcMock.Server.dll"]
