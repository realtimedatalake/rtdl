FROM alpine:latest

WORKDIR /hive-db-init

RUN apk add --no-cache postgresql-client

COPY /scripts/create-user.postgres.sql /hive-db-init/create-user.postgres.sql
COPY /scripts/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]