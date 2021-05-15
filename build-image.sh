#!/bin/bash

echo ""

echo -e "\nbuild docker hadoop image\n"
sudo docker build -t g2-pygitic/hadoop .

echo ""