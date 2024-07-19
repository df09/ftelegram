#!/bin/bash
echo "====================" >> log.txt
./venv/bin/python init.py | tee -a log.txt
