#!/bin/sh

psql -h yugabyte -p 5433 -U yugabyte -f ./create-user.postgres.sql
