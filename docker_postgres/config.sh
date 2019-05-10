#!/bin/bash
set -e

sed -ri "s/#log_statement = 'none'/log_statement = 'all'/g" /var/lib/postgresql/data/postgresql.conf
sed -ri "s/#log_connections = off/log_connections = on/g" /var/lib/postgresql/data/postgresql.conf
sed -ri "s/#log_disconnections = off/log_disconnections = on/g" /var/lib/postgresql/data/postgresql.conf
