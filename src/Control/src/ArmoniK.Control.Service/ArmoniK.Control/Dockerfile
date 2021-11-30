#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["src/Control/src/ArmoniK.Control.Service/ArmoniK.Control/ArmoniK.Control.csproj", "src/Control/src/ArmoniK.Control.Service/ArmoniK.Control/"]
COPY ["src/Core/src/ArmoniK.Core.csproj", "src/Core/src/"]
COPY ["src/Adaptors/MongoDB/src/ArmoniK.Adapters.MongoDB/ArmoniK.Adapters.MongoDB.csproj", "src/Adaptors/MongoDB/src/ArmoniK.Adapters.MongoDB/"]
RUN SolutionDir=/src dotnet restore "src/Control/src/ArmoniK.Control.Service/ArmoniK.Control/ArmoniK.Control.csproj"
COPY . .
WORKDIR "/src/src/Control/src/ArmoniK.Control.Service/ArmoniK.Control"
RUN SolutionDir=/src dotnet build "ArmoniK.Control.csproj" -c Release -o /app/build

FROM build AS publish
RUN SolutionDir=/src dotnet publish "ArmoniK.Control.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ArmoniK.Control.dll"]