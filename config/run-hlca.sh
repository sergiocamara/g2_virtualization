#!/bin/bash

# create input files 
mkdir input

cp ~/g2-hlca.pig .

cat ~/img.txt | python3 ~/img_parser.py > ~/img_parsed.txt

cp ~/img_parsed.txt input/img_parsed.txt

#cp ~/block0.txt input/img_parsed.txt

# create input directory on HDFS
hadoop fs -mkdir -p /user/hadoop

# put input files to HDFS
hdfs dfs -put ./input/* /user/hadoop

mapred start historyserver --daemon

# run hlca pig 
pig -f g2-hlca.pig


