#!/bin/bash

set -e

sleep 60

echo "Iniciando ejecución del análisis histórico..."

jupyter nbconvert --to html --execute --output-dir=/app analisis.ipynb

echo "Análisis histórico ejecutado exitosamente."