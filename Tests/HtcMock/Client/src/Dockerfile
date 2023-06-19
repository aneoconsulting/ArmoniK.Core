#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["Tests/HtcMock/Client/src/ArmoniK.Samples.HtcMock.Client.csproj", "Tests/HtcMock/Client/src/"]
COPY ["Tests/Common/Client/src/ArmoniK.Core.Common.Tests.Client.csproj", "Tests/Common/Client/src/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
RUN dotnet restore "Tests/HtcMock/Client/src/ArmoniK.Samples.HtcMock.Client.csproj"
COPY ["Tests/HtcMock/Client/src", "Tests/HtcMock/Client/src"]
COPY ["Tests/Common/Client/src", "Tests/Common/Client/src"]
COPY ["Common/src", "Common/src"]
WORKDIR "/src/Tests/HtcMock/Client/src"
RUN dotnet build "ArmoniK.Samples.HtcMock.Client.csproj" -c Release -o /app/build

FROM build AS publish
ARG VERSION=1.0.0.0
RUN dotnet publish "ArmoniK.Samples.HtcMock.Client.csproj" -c Release -o /app/publish -p:PackageVersion=$VERSION -p:Version=$VERSION

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser

USER armonikuser
ENTRYPOINT ["dotnet", "ArmoniK.Samples.HtcMock.Client.dll"]
