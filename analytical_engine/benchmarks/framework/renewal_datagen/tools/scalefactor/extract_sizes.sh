#!/bin/bash

NUM_USERS="10000 20000 50000 80000 100000 250000 500000 1000000 3000000"
DATA_GENERATOR=/path/to/dbgen/
SIZE_STATISTICS_DIR=/path/to/size_statistics/folder
HADOOP_DIR=/path/to/hadoop
OUTPUT_DIR=/path/to/dbgen/output


cd $DATA_GENERATOR 
rm $SIZE_STATISTICS_DIR/size_statistics.txt
echo "Persons Bytes" >> $SIZE_STATISTICS_DIR/size_statistics.txt
for num_users in $NUM_USERS
do 
	echo "numPersons:$num_users" > $DATA_GENERATOR/params.ini
	echo "startYear:2010" >> $DATA_GENERATOR/params.ini
	echo "numYears:3" >> $DATA_GENERATOR/params.ini
	echo "serializerType:csv" >> $DATA_GENERATOR/params.ini
	echo "enableCompression:false" >> $DATA_GENERATOR/params.ini
	echo "outputDir:$OUTPUT_DIR" >> $DATA_GENERATOR/params.ini
	sh run.sh
	$HADOOP_DIR/bin/hadoop fs -rm $OUTPUT_DIR/social_network/*.json
	SIZE=$($HADOOP_DIR/bin/hadoop fs -dus $OUTPUT_DIR/social_network | cut -f 2)
	echo "$num_users  $SIZE" >> $SIZE_STATISTICS_DIR/size_statistics.txt
done 

