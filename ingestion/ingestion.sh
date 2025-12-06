#!/bin/bash

echo "Iniciando Script 1..."
python -u ingestion.py &

echo "Iniciando Script 2..."
python -u ingestion2.py &

wait