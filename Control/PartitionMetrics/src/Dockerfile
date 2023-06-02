ARG BASE_IMAGE
ARG VERSION=1.0.0.0
FROM $BASE_IMAGE:$VERSION AS base

FROM mcr.microsoft.com/dotnet/aspnet:6.0
WORKDIR /app
COPY --from=base /app/publish/partition_metrics .
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
USER armonikuser

ENV ASPNETCORE_URLS http://+:1080
EXPOSE 1080

ENTRYPOINT ["dotnet", "ArmoniK.Core.Control.PartitionMetrics.dll"]
