#!/bin/bash

ECHO=/bin/echo

if [ $# -ne 1 ]
then
   $ECHO "Arguments not correctly supplied"
   $ECHO "Usage: sh testDatasets <dataset_dir>"
   exit
fi

#FILES="comment forum_containerOf_post person_email_emailaddress  person_studyAt_organisation post_isLocatedIn_place comment_hasCreator_person forum_hasMember_person           person_hasInterest_tag person_workAt_organisation tag comment_hasTag_tag forum_hasModerator_person person_isLocatedIn_place place tagclass comment_isLocatedIn_place forum_hasTag_tag person_knows_person place_isPartOf_place tagclass_isSubclassOf_tagclass comment_replyOf_comment organisation person_likes_comment post tag_hasType_tagclass comment_replyOf_post organisation_isLocatedIn_place person_likes_post post_hasCreator_person forum person person_speaks_language post_hasTag_tag"
ENTITIES="comment forum organisation person place post tag tagclass"
RELATIONS="comment_hasCreator_person comment_hasTag_tag comment_isLocatedIn_place comment_replyOf_comment comment_replyOf_post forum_containerOf_post forum_hasMember_person forum_hasModerator_person forum_hasTag_tag organisation_isLocatedIn_place person_isLocatedIn_place person_hasInterest_tag person_knows_person person_likes_comment person_likes_post person_studyAt_organisation person_workAt_organisation place_isPartOf_place    post_hasCreator_person post_hasTag_tag post_isLocatedIn_place tag_hasType_tagclass tagclass_isSubclassOf_tagclass"
REST="person_email_emailaddress person_speaks_language"

DIR=$1
TOTAL_BYTES=0
TOTAL_ENTITIES=0
TOTAL_RELATIONS=0

$ECHO "\begin{table}[H]"
$ECHO "\centering"
$ECHO "\begin{tabular} {| l | c | c |}"
$ECHO "\hline"
$ECHO "\textbf{Entity} & \textbf{Num Entities} & \textbf{Bytes} \\\\"
$ECHO "\hline"
$ECHO "\hline"

for file in $ENTITIES
do
    NUM_LINES=0 
    NUM_BYTES=0
    for aux_file in `ls $DIR/${file}_?.csv`
    do
        DATA=$(wc $aux_file | awk {'print $1 " " $3'})
        AUX_NUM_LINES=$($ECHO $DATA | cut -f1 -d' ') 
        AUX_NUM_BYTES=$($ECHO $DATA | cut -f2 -d' ') 
        NUM_LINES=$( $ECHO $NUM_LINES + $AUX_NUM_LINES - 1| bc)
        NUM_BYTES=$( $ECHO $NUM_BYTES + $AUX_NUM_BYTES | bc)
    done
    TOTAL_BYTES=$( $ECHO $TOTAL_BYTES + $NUM_BYTES | bc)
    TOTAL_ENTITIES=$( $ECHO $TOTAL_ENTITIES + $NUM_LINES | bc)
    LINE=$($ECHO "$file & $NUM_LINES & $NUM_BYTES" | sed -r 's/_/\\_/g')
   $ECHO "$LINE \\\\"
   $ECHO "\hline"
done

$ECHO "\hline"
$ECHO "\textbf{Relation} & \textbf{Num Relations} & \textbf{Bytes} \\\\"
$ECHO "\hline"
$ECHO "\hline"
for file in $RELATIONS
do
   NUM_LINES=0 
   NUM_BYTES=0
    AUX_FILES=$(ls $DIR/${file}_?.csv)
    for aux_file in $AUX_FILES
    do
        DATA=$(wc $aux_file | awk {'print $1 " " $3'})
        AUX_NUM_LINES=$($ECHO $DATA | cut -f1 -d' ') 
        AUX_NUM_BYTES=$($ECHO $DATA | cut -f2 -d' ') 
        NUM_LINES=$( $ECHO $NUM_LINES + $AUX_NUM_LINES - 1| bc)
        NUM_BYTES=$( $ECHO $NUM_BYTES + $AUX_NUM_BYTES | bc)
    done
   TOTAL_BYTES=$( $ECHO $TOTAL_BYTES + $NUM_BYTES | bc)
   TOTAL_RELATIONS=$( $ECHO $TOTAL_RELATIONS + $NUM_LINES | bc)
    LINE=$($ECHO "$file & $NUM_LINES & $NUM_BYTES" | sed -r 's/_/\\_/g')
   $ECHO "$LINE \\\\"
   $ECHO "\\hline"
done

$ECHO "\hline"
$ECHO "\textbf{Property Files} & \textbf{Num Properties} & \textbf{Bytes} \\\\"
$ECHO "\hline"
$ECHO "\hline"
for file in $REST
do
   NUM_LINES=0 
   NUM_BYTES=0
    AUX_FILES=$(ls $DIR/${file}_?.csv)
    for aux_file in $AUX_FILES
    do
        DATA=$(wc $aux_file | awk {'print $1 " " $3'})
        AUX_NUM_LINES=$($ECHO $DATA | cut -f1 -d' ') 
        AUX_NUM_BYTES=$($ECHO $DATA | cut -f2 -d' ') 
        NUM_LINES=$( $ECHO $NUM_LINES + $AUX_NUM_LINES - 1| bc)
        NUM_BYTES=$( $ECHO $NUM_BYTES + $AUX_NUM_BYTES | bc)
    done
   TOTAL_BYTES=$( $ECHO $TOTAL_BYTES + $NUM_BYTES | bc)
    LINE=$($ECHO "$file & $NUM_LINES & $NUM_BYTES" | sed -r 's/_/\\_/g')
   $ECHO "$LINE \\\\"
   $ECHO "\hline"
done

$ECHO "\hline"
$ECHO "\textbf{Total Entities} & \textbf{Total Relations} & \textbf{Total Bytes} \\\\"
$ECHO "\hline"
$ECHO "\hline"

$ECHO " $TOTAL_ENTITIES & $TOTAL_RELATIONS & $TOTAL_BYTES \\\\"
$ECHO "\hline"

$ECHO "\end{tabular}"
$ECHO "\end{table}"
