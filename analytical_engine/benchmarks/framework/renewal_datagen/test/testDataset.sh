#!/bin/bash

if [ $# -ne 1 ]
then
   echo "Arguments not correctly supplied"
   echo "Usage: sh testDataset <dir>"
   exit
fi

python2 ./validateKnowsGraph.py $1
