#!/bin/sh

sleep 15
psql -h yugabyte -p 5433 -U yugabyte -f ./create-user.postgres.sql
