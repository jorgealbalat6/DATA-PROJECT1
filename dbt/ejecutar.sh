#!/bin/bash

# Bucle infinito
while true; do
  echo "Iniciando dbt run..."
  dbt run
  echo "Esperando 1 hora (4000 segundos)..."
  sleep 4000 
done