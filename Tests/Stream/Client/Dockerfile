#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["Tests/Stream/Client/ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj", "Tests/Stream/Client/"]
COPY ["Tests/Stream/Common/ArmoniK.Extensions.Common.StreamWrapper.Tests.Common.csproj", "Tests/Stream/Common/"]
COPY ["Stream/src/ArmoniK.Core.Stream.csproj", "Stream/src/"]
RUN dotnet restore "Tests/Stream/Client/ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj"
COPY . .
WORKDIR "/src/Tests/Stream/Client"
RUN dotnet build "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.dll"]