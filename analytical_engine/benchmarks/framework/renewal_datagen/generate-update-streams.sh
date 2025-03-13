#!/bin/bash

set -euo pipefail

mkdir -p ../datagen-graphs

export ZSTD_NBTHREADS=`nproc`
export ZSTD_CLEVEL=12

for SF in 0.1 0.3 1 3 10 30 100 300 1000; do
    echo SF: ${SF}

    for NUM_UPDATE_PARTITIONS in 1 2 4 8 16 32 64 128 256 512 1024 12 24 48 96 192 384 768; do
        echo NUM_UPDATE_PARTITIONS: ${NUM_UPDATE_PARTITIONS}

        # create configuration file
        echo > params.ini
        echo ldbc.snb.datagen.generator.scaleFactor:snb.interactive.${SF} >> params.ini
        echo ldbc.snb.datagen.serializer.numUpdatePartitions:${NUM_UPDATE_PARTITIONS} >> params.ini
        echo ldbc.snb.datagen.serializer.dynamicActivitySerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.CsvBasicDynamicActivitySerializer >> params.ini
        echo ldbc.snb.datagen.serializer.dynamicPersonSerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.CsvBasicDynamicPersonSerializer >> params.ini
        echo ldbc.snb.datagen.serializer.staticSerializer:ldbc.snb.datagen.serializer.snb.csv.staticserializer.CsvBasicStaticSerializer >> params.ini
        echo ldbc.snb.datagen.parametergenerator.parameters:false >> params.ini

        # run datagen
        ./run.sh

        # only save the update stream files and the input configuration to the result directory
        mkdir -p ../datagen-graphs/social_network-sf${SF}-numpart-${NUM_UPDATE_PARTITIONS}/
        mv params.ini ../datagen-graphs/social_network-sf${SF}-numpart-${NUM_UPDATE_PARTITIONS}/
        mv social_network/updateStream* ../datagen-graphs/social_network-sf${SF}-numpart-${NUM_UPDATE_PARTITIONS}/
        mv social_network/.updateStream* ../datagen-graphs/social_network-sf${SF}-numpart-${NUM_UPDATE_PARTITIONS}/

        # compress the result directory using zstd
        tar --zstd -cvf ../datagen-graphs/social_network-sf${SF}-numpart-${NUM_UPDATE_PARTITIONS}.tar.zst ../datagen-graphs/social_network-sf${SF}-numpart-${NUM_UPDATE_PARTITIONS}/
    done
done
