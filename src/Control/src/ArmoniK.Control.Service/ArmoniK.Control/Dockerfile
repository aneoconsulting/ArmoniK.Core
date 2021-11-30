#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
RUN apt update && apt install -y protobuf-compiler
RUN dotnet tool install -g dotnet-grpc
ENV PATH="$PATH:/root/.dotnet/tools"
WORKDIR /armonik
COPY . .

WORKDIR src/Core/src/
RUN dotnet-grpc add-file -p ArmoniK.Core.csproj ../../Protos/V1/Compute.proto -s Client
RUN dotnet-grpc add-file -p ArmoniK.Core.csproj ../../Protos/V1/ClientService.proto -s Server
RUN dotnet-grpc add-file -p ArmoniK.Core.csproj ../../Protos/V1/Objects.proto -s Both
RUN dotnet-grpc add-file -p ArmoniK.Core.csproj ../../Protos/V1/TaskStatus.proto -s Both
RUN dotnet build ArmoniK.Core.csproj -c Release

WORKDIR /armonik/src/Control/src/ArmoniK.Control.Service/ArmoniK.Control/
RUN dotnet publish ArmoniK.Control.csproj -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "ArmoniK.Control.dll"]