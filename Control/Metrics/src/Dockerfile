#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
RUN groupadd --gid 5000 runuser && useradd --home-dir /home/runuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null runuser
USER runuser
COPY ["Control/Metrics/src/ArmoniK.Core.Control.Metrics.csproj", "Control/Metrics/src/"]
COPY ["Adaptors/Redis/src/ArmoniK.Core.Adapters.Redis.csproj", "Adaptors/Redis/src/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
COPY ["Adaptors/Amqp/src/ArmoniK.Core.Adapters.Amqp.csproj", "Adaptors/Amqp/src/"]
COPY ["Adaptors/MongoDB/src/ArmoniK.Core.Adapters.MongoDB.csproj", "Adaptors/MongoDB/src/"]
RUN dotnet restore "Control/Metrics/src/ArmoniK.Core.Control.Metrics.csproj"
COPY . .
WORKDIR "/src/Control/Metrics/src"
RUN dotnet build "ArmoniK.Core.Control.Metrics.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "ArmoniK.Core.Control.Metrics.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ArmoniK.Core.Control.Metrics.dll"]