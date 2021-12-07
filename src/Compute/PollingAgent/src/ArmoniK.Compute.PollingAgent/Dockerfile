#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["src/Compute/PollingAgent/src/ArmoniK.Compute.PollingAgent/ArmoniK.Compute.PollingAgent.csproj", "src/Compute/PollingAgent/src/ArmoniK.Compute.PollingAgent/"]
COPY ["src/Core/src/ArmoniK.Core.csproj", "src/Core/src/"]
COPY ["src/Adaptors/MongoDB/src/ArmoniK.Adapters.MongoDB/ArmoniK.Adapters.MongoDB.csproj", "src/Adaptors/MongoDB/src/ArmoniK.Adapters.MongoDB/"]
RUN dotnet restore "src/Compute/PollingAgent/src/ArmoniK.Compute.PollingAgent/ArmoniK.Compute.PollingAgent.csproj"
COPY . .
WORKDIR "/src/src/Compute/PollingAgent/src/ArmoniK.Compute.PollingAgent"
RUN dotnet build "ArmoniK.Compute.PollingAgent.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "ArmoniK.Compute.PollingAgent.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ArmoniK.Compute.PollingAgent.dll"]