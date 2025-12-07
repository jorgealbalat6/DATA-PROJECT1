#!/bin/bash
echo "Esperando 180 segundos iniciales de cortesía para arrancar..."
sleep 180
# Bucle infinito
while true; do
  echo "Iniciando dbt run..."
  cd proyecto_dbt
  dbt run --profiles-dir .
  cd ..
  echo "Esperando 1 hora (3600 segundos)..."
  sleep 3600 
done