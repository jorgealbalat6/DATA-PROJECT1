#!/bin/bash

echo "Iniciando ejecución del análisis histórico..."

jupyter nbconvert --to html --execute --no-input --output-dir=/app analisis.ipynb

echo "Análisis histórico ejecutado exitosamente."