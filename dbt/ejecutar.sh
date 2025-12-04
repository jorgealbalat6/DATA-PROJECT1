#!/bin/bash

# Bucle infinito
while true; do
  echo "Iniciando dbt run..."
  cd proyecto_dbt
  dbt run --profiles-dir .
  cd ..
  echo "Esperando 1 hora (4000 segundos)..."
  sleep 4000 
done