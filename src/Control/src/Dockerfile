#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["src/Control/src/ArmoniK.Control.csproj", "src/Control/src/"]
COPY ["src/Core/src/ArmoniK.Core.csproj", "src/Core/src/"]
COPY ["src/Adaptors/Amqp/src/ArmoniK.Adapters.Amqp.csproj", "src/Adaptors/Amqp/src/"]
COPY ["src/Adaptors/MongoDB/src/ArmoniK.Adapters.MongoDB.csproj", "src/Adaptors/MongoDB/src/"]
RUN dotnet restore "src/Control/src/ArmoniK.Control.csproj"
COPY . .
WORKDIR "/src/src/Control/src"
RUN dotnet build "ArmoniK.Control.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "ArmoniK.Control.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ArmoniK.Control.dll"]