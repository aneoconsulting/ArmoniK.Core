#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS base-linux
USER $APP_UID
ENTRYPOINT [ "dotnet" ]

FROM mcr.microsoft.com/dotnet/sdk:10.0-nanoserver-ltsc2022 AS base-windows
ENTRYPOINT ["C:\\Program Files\\dotnet\\dotnet.exe"]

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG TARGETARCH
ARG TARGETOS
ARG VERSION=1.0.0.0

WORKDIR /src
COPY ["Tests/Stream/Client/ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj", "Tests/Stream/Client/"]
COPY ["Tests/Stream/Common/ArmoniK.Extensions.Common.StreamWrapper.Tests.Common.csproj", "Tests/Stream/Common/"]
COPY ["Utils/src/ArmoniK.Core.Utils.csproj", "Utils/src/"]
COPY ["Base/src/ArmoniK.Core.Base.csproj", "Base/src/"]
RUN dotnet restore -a "${TARGETARCH}" "Tests/Stream/Client/ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj"
COPY ["Tests/Stream/Client", "Tests/Stream/Client"]
COPY ["Tests/Stream/Common", "Tests/Stream/Common"]
COPY ["Utils/src/", "Utils/src/"]
COPY ["Base/src/", "Base/src/"]
WORKDIR "/src/Tests/Stream/Client"
RUN dotnet build "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.csproj" -a "${TARGETARCH}" -c Release -o /app/build -p:RunAnalyzers=false -p:WarningLevel=0


FROM base-${TARGETOS} as final
WORKDIR /app
COPY --from=build /app/build .
CMD ["test", "ArmoniK.Extensions.Common.StreamWrapper.Tests.Client.dll"]
