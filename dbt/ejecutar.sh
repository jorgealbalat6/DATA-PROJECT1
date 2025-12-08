#!/bin/bash

sleep 90

set -e
echo "Iniciando dbt run..."
cd proyecto_dbt
dbt run --profiles-dir .
cd ..
echo "Exito dbt"

