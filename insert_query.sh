#!/bin/bash

DB="testdb"
USER="postgres"
export PGPASSWORD="password"

psql -U "$USER" -d "$DB" -c "DELETE from mono_signal"
psql -U "$USER" -d "$DB" -c "DELETE  from tower_signal"
for file in $(pwd)/output_data/mono_signal/*.csv; do
    echo "Loading $file into mono_signal"
    psql -U "$USER" -d "$DB" -c "\COPY mono_signal FROM '$file' CSV HEADER;" 
done

# TOWER SIGNAL
for file in $(pwd)/output_data/tower_signal/*.csv; do
    echo "Loading $file into tower_signal"
    psql -U "$USER" -d "$DB" -c "\COPY tower_signal FROM '$file' CSV HEADER;"
done


