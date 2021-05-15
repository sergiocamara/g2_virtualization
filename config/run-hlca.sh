#!/bin/bash

# create input files 
mkdir input

# process image
cat ~/img.txt | python3 ~/img_parser.py > ~/input/img_parsed.txt

# create input directory on HDFS
hadoop fs -mkdir -p /user/hadoop

# put input files to HDFS
hdfs dfs -put ./input/* /user/hadoop

$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

# run hlca pig 
pig -f g2-hlca.pig
