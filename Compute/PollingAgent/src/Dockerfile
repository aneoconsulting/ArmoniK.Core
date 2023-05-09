#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["Compute/PollingAgent/src/ArmoniK.Core.Compute.PollingAgent.csproj", "Compute/PollingAgent/src/"]
COPY ["Utils/src/ArmoniK.Core.Utils.csproj", "Utils/src/"]
COPY ["Base/src/ArmoniK.Core.Base.csproj", "Base/src/"]
COPY ["Adaptors/LocalStorage/src/ArmoniK.Core.Adapters.LocalStorage.csproj", "Adaptors/LocalStorage/src/"]
COPY ["Adaptors/Redis/src/ArmoniK.Core.Adapters.Redis.csproj", "Adaptors/Redis/src/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
COPY ["Adaptors/MongoDB/src/ArmoniK.Core.Adapters.MongoDB.csproj", "Adaptors/MongoDB/src/"]
COPY ["Adaptors/S3/src/ArmoniK.Core.Adapters.S3.csproj", "Adaptors/S3/src/"]
RUN dotnet restore "Compute/PollingAgent/src/ArmoniK.Core.Compute.PollingAgent.csproj"
COPY . .
WORKDIR "/src/Compute/PollingAgent/src"
RUN dotnet build "ArmoniK.Core.Compute.PollingAgent.csproj" -c Release -o /app/build

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS adapters
WORKDIR /src
COPY ["Utils/src/ArmoniK.Core.Utils.csproj", "Utils/src/"]
COPY ["Base/src/ArmoniK.Core.Base.csproj", "Base/src/"]
COPY ["Adaptors/RabbitMQ/src/ArmoniK.Core.Adapters.RabbitMQ.csproj", "Adaptors/RabbitMQ/src/"]
COPY ["Adaptors/Amqp/src/ArmoniK.Core.Adapters.Amqp.csproj", "Adaptors/Amqp/src/"]
COPY ["Adaptors/QueueCommon/src/ArmoniK.Core.Adapters.QueueCommon.csproj", "Adaptors/QueueCommon/src/"]
RUN dotnet restore "Adaptors/RabbitMQ/src/ArmoniK.Core.Adapters.RabbitMQ.csproj"
RUN dotnet restore "Adaptors/Amqp/src/ArmoniK.Core.Adapters.Amqp.csproj"
COPY . .
WORKDIR "/src/Adaptors/RabbitMQ/src"
RUN dotnet build "ArmoniK.Core.Adapters.RabbitMQ.csproj" -c Release -o /app/build/rabbit
WORKDIR "/src/Adaptors/Amqp/src"
RUN dotnet build "ArmoniK.Core.Adapters.Amqp.csproj" -c Release -o /app/build/amq

FROM build AS publish
RUN dotnet publish "ArmoniK.Core.Compute.PollingAgent.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM adapters AS publisha
WORKDIR "/src/Adaptors/RabbitMQ/src"
RUN dotnet publish "ArmoniK.Core.Adapters.RabbitMQ.csproj" -c Release -o /app/publish/rabbit /p:UseAppHost=false
WORKDIR "/src/Adaptors/Amqp/src"
RUN dotnet publish "ArmoniK.Core.Adapters.Amqp.csproj" -c Release -o /app/publish/amqp /p:UseAppHost=false


FROM base AS final
WORKDIR /adapters/queue/amqp
COPY --from=publisha /app/publish/amqp .
WORKDIR /adapters/queue/rabbit
COPY --from=publisha /app/publish/rabbit .
WORKDIR /app
COPY --from=publish /app/publish .
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
RUN mkdir /cache /local_storage && chown armonikuser: /cache /local_storage
USER armonikuser

ENV ASPNETCORE_URLS http://+:1080
EXPOSE 1080

ENTRYPOINT ["dotnet", "ArmoniK.Core.Compute.PollingAgent.dll"]
