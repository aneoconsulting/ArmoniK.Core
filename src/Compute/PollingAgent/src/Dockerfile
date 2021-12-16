#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["src/Compute/PollingAgent/src/ArmoniK.Compute.PollingAgent.csproj", "src/Compute/PollingAgent/src/"]
COPY ["src/Core/src/ArmoniK.Core.csproj", "src/Core/src/"]
COPY ["src/Adaptors/MongoDB/src/ArmoniK.Adapters.MongoDB.csproj", "src/Adaptors/MongoDB/src/"]
COPY ["src/Adaptors/Amqp/src/ArmoniK.Adapters.Amqp.csproj", "src/Adaptors/Amqp/src/"]

RUN dotnet restore "src/Compute/PollingAgent/src/ArmoniK.Compute.PollingAgent.csproj"
COPY . .
WORKDIR "/src/src/Compute/PollingAgent/src"
RUN dotnet build "ArmoniK.Compute.PollingAgent.csproj" -c Debug -o /app/build

FROM build AS publish
RUN dotnet publish "ArmoniK.Compute.PollingAgent.csproj" -c Debug -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ArmoniK.Compute.PollingAgent.dll"]