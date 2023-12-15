#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS base

FROM base AS build
WORKDIR /src
COPY ["Tests/Stream/Client/ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj", "Tests/Stream/Client/"]
COPY ["Tests/Stream/Common/ArmoniK.Extensions.Common.StreamWrapper.Tests.Common.csproj", "Tests/Stream/Common/"]
COPY ["Utils/src/ArmoniK.Core.Utils.csproj", "Utils/src/"]
COPY ["Base/src/ArmoniK.Core.Base.csproj", "Base/src/"]
COPY ["Common/src/ArmoniK.Core.Common.csproj", "Common/src/"]
RUN dotnet restore "Tests/Stream/Client/ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj"
COPY ["Tests/Stream/Client", "Tests/Stream/Client"]
COPY ["Tests/Stream/Common", "Tests/Stream/Common"]
COPY ["Utils/src/", "Utils/src/"]
COPY ["Base/src/", "Base/src/"]
COPY ["Common/src", "Common/src"]
WORKDIR "/src/Tests/Stream/Client"
RUN dotnet build "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj" -c Release -o /app/build -p:RunAnalyzers=false -p:WarningLevel=0


FROM base as final
WORKDIR /app
COPY --from=build /app/build .
RUN groupadd --gid 5000 armonikuser \
 && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser \ 
 && chown -R armonikuser: /app \
 && rm -rf /tmp/NuGetScratch
USER armonikuser
ENTRYPOINT ["dotnet", "test", "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.dll"]
