#!/bin/bash
set -e

response=$(psql -h 0.0.0.0 -U postgres -tc "SELECT 1 FROM pg_roles WHERE rolname='validator'")
if [ "$(echo "$response" | xargs)" != "1" ]; then
    echo "Пользователь validator не существует."
    psql -h 0.0.0.0 -U postgres -c "CREATE USER validator WITH PASSWORD 'val1dat0r';"
fi
response=$(psql -h 0.0.0.0 -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'project-sem-1'")
if [ "$(echo "$response" | xargs)" != "1" ]; then
    echo "База данных project-sem-1 не существует."
    psql -h 0.0.0.0 -U postgres -c "CREATE DATABASE project-sem-1 OWNER validator;"
fi
psql -h 0.0.0.0 -U validator -d project-sem-1 -c "CREATE TABLE IF NOT EXISTS prices (id SERIAL PRIMARY KEY, created_at DATE NOT NULL, name TEXT NOT NULL, category TEXT NOT NULL, price INTEGER NOT NULL);"
psql -h 0.0.0.0 -U validator -d project-sem-1 -c "TRUNCATE TABLE prices;"
echo "База данных подготовлена."

go mod init sem1-project
go get -u github.com/lib/pq
echo "Зависимости установлены."