ARG BASE_IMAGE
ARG VERSION=1.0.0.0
FROM $BASE_IMAGE:$VERSION AS base

FROM mcr.microsoft.com/dotnet/aspnet:6.0
WORKDIR /adapters/queue/amqp
COPY --from=base /app/publish/amqp .
WORKDIR /adapters/queue/rabbit
COPY --from=base /app/publish/rabbit .
WORKDIR /app
COPY --from=base /app/publish/submitter .
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
RUN mkdir /local_storage && chown armonikuser: /local_storage
USER armonikuser

ENV ASPNETCORE_URLS http://+:1080, http://+:1081
EXPOSE 1080
EXPOSE 1081

ENTRYPOINT ["dotnet", "ArmoniK.Core.Control.Submitter.dll"]
