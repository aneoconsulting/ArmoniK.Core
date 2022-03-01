#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["Common/StreamWrapper/tests/Client/ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj", "Common/StreamWrapper/tests/Client/"]
COPY ["Common/StreamWrapper/tests/Common/ArmoniK.Extensions.Common.StreamWrapper.Tests.Common.csproj", "Common/StreamWrapper/tests/Common/"]
COPY ["Common/StreamWrapper/src/ArmoniK.Extensions.Common.StreamWrapper.csproj", "Common/StreamWrapper/src/"]
RUN dotnet restore "Common/StreamWrapper/tests/Client/ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj"
COPY . .
WORKDIR "/src/Common/StreamWrapper/tests/Client"
RUN dotnet build "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.dll"]