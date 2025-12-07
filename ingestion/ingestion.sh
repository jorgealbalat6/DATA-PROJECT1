#!/bin/bash

echo "Iniciando Script de Madrid..."
python -u ingestionMAD.py &

echo "Iniciando Script de Barcelona..."
python -u ingestionBCN.py &

wait