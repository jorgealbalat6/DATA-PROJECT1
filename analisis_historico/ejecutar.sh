#!/bin/bash

set -e

echo "Iniciando ejecución del análisis histórico..."

jupyter nbconvert --to html --execute --output-dir=/app analisis.ipynb

echo "Análisis histórico ejecutado exitosamente."