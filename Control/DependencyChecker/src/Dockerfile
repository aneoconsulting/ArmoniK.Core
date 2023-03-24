#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["Control/DependencyChecker/src/ArmoniK.Core.Control.DependencyChecker.csproj", "Control/DependencyChecker/src/"]
COPY ["Utils/src/ArmoniK.Core.Utils.csproj", "Utils/src/"]
COPY ["Base/src/ArmoniK.Core.Base.csproj", "Base/src/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
COPY ["Adaptors/MongoDB/src/ArmoniK.Core.Adapters.MongoDB.csproj", "Adaptors/MongoDB/src/"]
RUN dotnet restore "Control/DependencyChecker/src/ArmoniK.Core.Control.DependencyChecker.csproj"
COPY . .
WORKDIR "/src/Control/DependencyChecker/src"
RUN dotnet build "ArmoniK.Core.Control.DependencyChecker.csproj" -c Release -o /app/build

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS adapters
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
RUN dotnet publish "ArmoniK.Core.Control.DependencyChecker.csproj" -c Release -o /app/publish

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
RUN mkdir /local_storage && chown armonikuser: /local_storage
USER armonikuser

ENV ASPNETCORE_URLS http://+:1080, http://+:1081
EXPOSE 1080
EXPOSE 1081

ENTRYPOINT ["dotnet", "ArmoniK.Core.Control.DependencyChecker.dll"]
