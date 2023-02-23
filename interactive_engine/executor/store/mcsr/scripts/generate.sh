#!/bin/bash

SRC_DIR=$1
DST_DIR=$2

function id_column() {
	INPUT_FILE=$1
	shift
	ID_COLUMN=$1
	shift
	OUTPUT_FILE=$1
	shift
	COLUMNS=$@

	cut -d '|' -f $ID_COLUMN $INPUT_FILE > tmp.id
	cut -d '|' -f $COLUMNS $INPUT_FILE > tmp.remaining
	paste -d '|' tmp.id tmp.remaining | sort -k1n > $OUTPUT_FILE
}

function vertex_file() {
	PREFIX=$1
	shift
	ID_COLUMN=$1
	shift
	OUTPUT_FILE=$1
	shift
	COLUMNS=$@

	cat $PREFIX/*.csv > tmp
	id_column tmp $ID_COLUMN $OUTPUT_FILE $@
}

vertex_file $SRC_DIR/dynamic/Comment 2 $DST_DIR/COMMENT 1,6
vertex_file $SRC_DIR/dynamic/Forum 2 $DST_DIR/FORUM 1,3
vertex_file $SRC_DIR/dynamic/Post 2 $DST_DIR/POST 1,6,8
vertex_file $SRC_DIR/static/Place 1 $DST_DIR/PLACE 2,3,4
vertex_file $SRC_DIR/static/Tag 1 $DST_DIR/TAG 2,3
vertex_file $SRC_DIR/dynamic/Person 2 $DST_DIR/PERSON 1,3,4,5,6,7,8,9,10
vertex_file $SRC_DIR/static/Organisation 1 $DST_DIR/ORGANISATION 2,3,4
vertex_file $SRC_DIR/static/Tagclass 1 $DST_DIR/TAGCLASS 2,3

function convert {
	cat $SRC_DIR/dynamic/$1/*.csv | cut -d '|' -f 2- | sort -k1n > $DST_DIR/$2
}

function convert_person_place {
	cat $SRC_DIR/dynamic/$1/*.csv | cut -d '|' -f 2,3 | sort -k1n > $DST_DIR/$2
}

convert Comment_hasCreator_Person COMMENT_HASCREATOR_PERSON
convert Comment_hasTag_Tag COMMENT_HASTAG_TAG
convert Comment_isLocatedIn_Country COMMENT_ISLOCATEDIN_PLACE
convert Comment_replyOf_Comment COMMENT_REPLYOF_COMMENT
convert Comment_replyOf_Post COMMENT_REPLYOF_POST
# sort -k1n $SRC_DIR/dynamic/Comment_hasCreator_Person/*.csv > $DST_DIR/COMMENT_HASCREATOR_PERSON
# sort -k1n $SRC_DIR/dynamic/Comment_hasTag_Tag/*.csv > $DST_DIR/COMMENT_HASTAG_TAG
# sort -k1n $SRC_DIR/dynamic/Comment_isLocatedIn_Country/*.csv > $DST_DIR/COMMENT_ISLOCATEDIN_PLACE
# sort -k1n $SRC_DIR/dynamic/Comment_replyOf_Comment/*.csv > $DST_DIR/COMMENT_REPLYOF_COMMENT
# sort -k1n $SRC_DIR/dynamic/Comment_replyOf_Post/*.csv > $DST_DIR/COMMENT_REPLYOF_POST

convert Forum_hasModerator_Person FORUM_HASMODERATOR_PERSON
convert Forum_hasTag_Tag FORUM_HASTAG_TAG
convert Forum_containerOf_Post FORUM_CONTAINEROF_POST
# sort -k1n $SRC_DIR/dynamic/Forum_containerOf_Post/*.csv > $DST_DIR/FORUM_CONTAINEROF_POST
convert Forum_hasMember_Person FORUM_HASMEMBER_PERSON
# sort -k1n $SRC_DIR/dynamic/Forum_hasMember_Person/*.csv > $DST_DIR/FORUM_HASMEMBER_PERSON
# sort -k1n $SRC_DIR/dynamic/Forum_hasModerator_Person/*.csv > $DST_DIR/FORUM_HASMODERATOR_PERSON
# sort -k1n $SRC_DIR/dynamic/Forum_hasTag_Tag/*.csv > $DST_DIR/FORUM_HASTAG_TAG

convert Person_hasInterest_Tag PERSON_HASINTEREST_TAG
convert Person_isLocatedIn_City PERSON_ISLOCATEDIN_PLACE
convert_person_place Person_studyAt_University PERSON_STUDYAT_ORGANISATION
convert_person_place Person_workAt_Company PERSON_WORKAT_ORGANISATION
# sort -k1n $SRC_DIR/dynamic/Person_hasInterest_Tag/*.csv > $DST_DIR/PERSON_HASINTEREST_TAG
# sort -k1n $SRC_DIR/dynamic/Person_isLocatedIn_City/*.csv > $DST_DIR/PERSON_ISLOCATEDIN_PLACE
# sort -k1n $SRC_DIR/dynamic/Person_knows_Person/*.csv > $DST_DIR/PERSON_KNOWS_PERSON
convert Person_knows_Person PERSON_KNOWS_PERSON
convert Person_likes_Comment PERSON_LIKES_COMMENT
convert Person_likes_Post PERSON_LIKES_POST
# sort -k1n $SRC_DIR/dynamic/Person_likes_Comment/*.csv > $DST_DIR/PERSON_LIKES_COMMENT
# sort -k1n $SRC_DIR/dynamic/Person_likes_Post/*.csv > $DST_DIR/PERSON_LIKES_POST
# sort -k1n $SRC_DIR/dynamic/Person_studyAt_University/*.csv > $DST_DIR/PERSON_STUDYAT_ORGANISATION
# sort -k1n $SRC_DIR/dynamic/Person_workAt_Company/*.csv > $DST_DIR/PERSON_WORKAT_ORGANISATION

convert Post_hasCreator_Person POST_HASCREATOR_PERSON
convert Post_hasTag_Tag POST_HASTAG_TAG
convert Post_isLocatedIn_Country POST_ISLOCATEDIN_PLACE

# sort -k1n $SRC_DIR/dynamic/Post_hasCreator_Person/*.csv > $DST_DIR/POST_HASCREATOR_PERSON
# sort -k1n $SRC_DIR/dynamic/Post_hasTag_Tag/*.csv > $DST_DIR/POST_HASTAG_TAG
# sort -k1n $SRC_DIR/dynamic/Post_isLocatedIn_Country/*.csv > $DST_DIR/POST_ISLOCATEDIN_PLACE
sort -k1n $SRC_DIR/static/Organisation_isLocatedIn_Place/*.csv > $DST_DIR/ORGANISATION_ISLOCATEDIN_PLACE
sort -k1n $SRC_DIR/static/Place_isPartOf_Place/*.csv > $DST_DIR/PLACE_ISPARTOF_PLACE
sort -k1n $SRC_DIR/static/Tag_hasType_Tagclass/*.csv > $DST_DIR/TAG_HASTYPE_TAGCLASS
sort -k1n $SRC_DIR/static/Tagclass_isSubclassOf_Tagclass/*.csv > $DST_DIR/TAGCLASS_ISSUBCLASSOF_TAGCLASS
