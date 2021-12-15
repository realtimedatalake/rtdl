#!/bin/sh

sleep 30
psql -h yugabyte -p 5433 -U yugabyte -f /hive-db-init/create-user.postgres.sql
