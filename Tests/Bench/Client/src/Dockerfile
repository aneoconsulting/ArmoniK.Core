#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["Tests/Bench/Client/src/ArmoniK.Samples.Bench.Client.csproj", "Tests/Bench/Client/src/"]
COPY ["Tests/Common/Client/src/ArmoniK.Core.Common.Tests.Client.csproj", "Tests/Common/Client/src/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
RUN dotnet restore "Tests/Bench/Client/src/ArmoniK.Samples.Bench.Client.csproj"
COPY . .
WORKDIR "/src/Tests/Bench/Client/src"
RUN dotnet build "ArmoniK.Samples.Bench.Client.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "ArmoniK.Samples.Bench.Client.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser

USER armonikuser
ENTRYPOINT ["dotnet", "ArmoniK.Samples.Bench.Client.dll"]
