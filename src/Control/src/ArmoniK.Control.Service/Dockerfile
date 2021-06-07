#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["Control/src/ArmoniK.Control.Service/ArmoniK.Control.Service.csproj", "Control/src/ArmoniK.Control.Service/"]
RUN dotnet restore "Control/src/ArmoniK.Control.Service/ArmoniK.Control.Service.csproj"
COPY . .
WORKDIR "/src/Control/src/ArmoniK.Control.Service"
RUN dotnet build "ArmoniK.Control.Service.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "ArmoniK.Control.Service.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ArmoniK.Control.Service.dll"]