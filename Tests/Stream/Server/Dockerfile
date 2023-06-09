#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:7.0 AS base
WORKDIR /app
ENV ASPNETCORE_URLS http://+:1080;https://+1443
EXPOSE 1080
EXPOSE 1443

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["Tests/Stream/Server/ArmoniK.Extensions.Common.StreamWrapper.Tests.Server.csproj", "Tests/Stream/Server/"]
COPY ["Tests/Stream/Common/ArmoniK.Extensions.Common.StreamWrapper.Tests.Common.csproj", "Tests/Stream/Common/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
RUN dotnet restore "Tests/Stream/Server/ArmoniK.Extensions.Common.StreamWrapper.Tests.Server.csproj"
COPY ["Tests/Stream/Server", "Tests/Stream/Server"]
COPY ["Tests/Stream/Common", "Tests/Stream/Common"]
COPY ["Common/src", "Common/src"]
WORKDIR "/src/Tests/Stream/Server"
RUN dotnet build "ArmoniK.Extensions.Common.StreamWrapper.Tests.Server.csproj" -c Release -o /app/build

FROM build AS publish
ARG VERSION=1.0.0.0
RUN dotnet publish "ArmoniK.Extensions.Common.StreamWrapper.Tests.Server.csproj" -c Release -o /app/publish -p:PackageVersion=$VERSION -p:Version=$VERSION

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
RUN mkdir /cache && chown armonikuser: /cache
USER armonikuser

ENTRYPOINT ["dotnet", "ArmoniK.Extensions.Common.StreamWrapper.Tests.Server.dll"]
