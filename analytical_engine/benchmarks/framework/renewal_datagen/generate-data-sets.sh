#!/bin/bash

set -euo pipefail

rm -rf ../datagen-graphs-regen
mkdir -p ../datagen-graphs-regen

export ZSTD_NBTHREADS=`nproc`
export ZSTD_CLEVEL=12
export NUM_UPDATE_PARTITIONS=1


# for SF in 0.1 0.3 1 3 10 30 100 300 1000; do
for SF in 10; do
    echo SF: ${SF}

    # # <turtle>
    # SERIALIZER=Turtle

    # # create configuration file
    # echo > params.ini
    # echo ldbc.snb.datagen.generator.scaleFactor:snb.interactive.${SF} >> params.ini
    # echo ldbc.snb.datagen.serializer.dynamicActivitySerializer:ldbc.snb.datagen.serializer.snb.turtle.${SERIALIZER}DynamicActivitySerializer >> params.ini
    # echo ldbc.snb.datagen.serializer.dynamicPersonSerializer:ldbc.snb.datagen.serializer.snb.turtle.${SERIALIZER}DynamicPersonSerializer >> params.ini
    # echo ldbc.snb.datagen.serializer.staticSerializer:ldbc.snb.datagen.serializer.snb.turtle.${SERIALIZER}StaticSerializer >> params.ini
    # echo ldbc.snb.datagen.serializer.numUpdatePartitions:${NUM_UPDATE_PARTITIONS} >> params.ini
    # echo ldbc.snb.datagen.parametergenerator.parameters:false >> params.ini

    # # run datagen
    # ./run.sh

    # # move the output to a separate directory
    # mv social_network ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}
    # mv params.ini ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}

    # # compress the result directory using zstd
    # cd ../datagen-graphs-regen
    # tar --zstd -cvf social_network-sf${SF}-${SERIALIZER}.tar.zst social_network-sf${SF}-${SERIALIZER}/

    # # cleanup
    # rm -rf social_network-sf${SF}-${SERIALIZER}/

    # # return and continue
    # cd ../ldbc_snb_datagen_hadoop/
    # rm -rf hadoop/
    # # </turtle>

    # for SERIALIZER in CsvBasic CsvComposite CsvCompositeMergeForeign CsvMergeForeign; do
    for SERIALIZER in CsvBasic; do
        echo SERIALIZER: ${SERIALIZER}
            
        # for DATEFORMATTER in StringDateFormatter LongDateFormatter; do
        for DATEFORMATTER in LongDateFormatter; do
            echo DATEFORMATTER: ${DATEFORMATTER}

            # create configuration file
            echo > params.ini
            echo ldbc.snb.datagen.generator.scaleFactor:snb.interactive.${SF} >> params.ini
            echo ldbc.snb.datagen.serializer.dynamicActivitySerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.${SERIALIZER}DynamicActivitySerializer >> params.ini
            echo ldbc.snb.datagen.serializer.dynamicPersonSerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.${SERIALIZER}DynamicPersonSerializer >> params.ini
            echo ldbc.snb.datagen.serializer.staticSerializer:ldbc.snb.datagen.serializer.snb.csv.staticserializer.${SERIALIZER}StaticSerializer >> params.ini
            echo ldbc.snb.datagen.serializer.dateFormatter:ldbc.snb.datagen.util.formatter.${DATEFORMATTER} >> params.ini
            echo ldbc.snb.datagen.serializer.numUpdatePartitions:${NUM_UPDATE_PARTITIONS} >> params.ini
            echo ldbc.snb.datagen.parametergenerator.parameters:false >> params.ini
            # added to generate static social networks
            echo ldbc.snb.datagen.serializer.updateStreams:false >> params.ini
            # added to ignore activity
            # note: enable activity to generate communities
            echo ldbc.snb.datagen.generator.activity:false >> params.ini
            
            # HopGenerator
            echo ldbc.snb.datagen.generator.knowsGenerator:ldbc.snb.datagen.generator.generators.knowsgenerators.DistanceHopKnowsGenerator >> params.ini
            # echo ldbc.snb.datagen.generator.knowsGenerator:ldbc.snb.datagen.generator.generators.knowsgenerators.DistanceKnowsGenerator >> params.ini

            # run datagen
            ./run.sh

            # move the output to a separate directory
            # mv social_network ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}
            # mv params.ini ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}
            # use hdfs command instead
            mkdir -p ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}
            mv params.ini ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}
            hdfs dfs -get social_network ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}
            # hdfs dfs -get social_network/dynamic/person_0_0.csv /home/sy429729/datagen-graphs-regen
            # hdfs dfs -get social_network/dynamic/person_knows_person_0_0.csv /home/sy429729/datagen-graphs-regen

            # compress the result directory using zstd
            # cd ../datagen-graphs-regen
            # tar --zstd -cvf social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}.tar.zst social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}/

            # cleanup
            # rm -rf social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}/

            # return and continue
            # cd ../ldbc_snb_datagen_hadoop/
            # rm -rf hadoop/
            #use hdfs command instead
            hdfs dfs -rm -r hadoop
            hdfs dfs -rm -r social_network
        done
    done
done
