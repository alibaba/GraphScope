COPY PLACE FROM "/data/ldbc10/place_0_0.csv"(HEADER=true, DELIM="|");
COPY PERSON FROM "/data/ldbc10/person_0_0.csv"(HEADER=true, DELIM="|");
COPY COMMENT FROM "/data/ldbc10/comment_0_0.csv"(HEADER=true, DELIM="|");
COPY POST FROM "/data/ldbc10/post_0_0.csv"(HEADER=true, DELIM="|");
COPY FORUM FROM "/data/ldbc10/forum_0_0.csv"(HEADER=true, DELIM="|");
COPY ORGANISATION FROM "/data/ldbc10/organisation_0_0.csv"(HEADER=true, DELIM="|");
COPY TAGCLASS FROM "/data/ldbc10/tagclass_0_0.csv"(HEADER=true, DELIM="|");
COPY TAG FROM "/data/ldbc10/tag_0_0.csv"(HEADER=true, DELIM="|");
COPY COMMENT_HASCREATOR_PERSON FROM "/data/ldbc10/comment_hasCreator_person_0_0.csv"(HEADER=true, DELIM="|");
COPY POST_HASCREATOR_PERSON FROM "/data/ldbc10/post_hasCreator_person_0_0.csv"(HEADER=true, DELIM="|");
COPY POST_HASTAG_TAG FROM "/data/ldbc10/post_hasTag_tag_0_0.csv"(HEADER=true, DELIM="|");
COPY FORUM_HASTAG_TAG FROM "/data/ldbc10/forum_hasTag_tag_0_0.csv"(HEADER=true, DELIM="|");
COPY COMMENT_HASTAG_TAG FROM "/data/ldbc10/comment_hasTag_tag_0_0.csv"(HEADER=true, DELIM="|");
COPY COMMENT_REPLYOF_COMMENT FROM "/data/ldbc10/comment_replyOf_comment_0_0.csv"(HEADER=true, DELIM="|");
COPY COMMENT_REPLYOF_POST FROM "/data/ldbc10/comment_replyOf_post_0_0.csv"(HEADER=true, DELIM="|");
COPY FORUM_CONTAINEROF_POST FROM "/data/ldbc10/forum_containerOf_post_0_0.csv"(HEADER=true, DELIM="|");
COPY FORUM_HASMEMBER_PERSON FROM "/data/ldbc10/forum_hasMember_person_0_0.csv"(HEADER=true, DELIM="|");
COPY FORUM_HASMODERATOR_PERSON FROM "/data/ldbc10/forum_hasModerator_person_0_0.csv"(HEADER=true, DELIM="|");
COPY PERSON_HASINTEREST_TAG FROM "/data/ldbc10/person_hasInterest_tag_0_0.csv"(HEADER=true, DELIM="|");
COPY COMMENT_ISLOCATEDIN_PLACE FROM "/data/ldbc10/comment_isLocatedIn_place_0_0.csv"(HEADER=true, DELIM="|");
COPY PERSON_ISLOCATEDIN_PLACE FROM "/data/ldbc10/person_isLocatedIn_place_0_0.csv"(HEADER=true, DELIM="|");
COPY POST_ISLOCATEDIN_PLACE FROM "/data/ldbc10/post_isLocatedIn_place_0_0.csv"(HEADER=true, DELIM="|");
COPY ORGANISATION_ISLOCATEDIN_PLACE FROM "/data/ldbc10/organisation_isLocatedIn_place_0_0.csv"(HEADER=true, DELIM="|");
COPY PERSON_KNOWS_PERSON FROM "/data/ldbc10/person_knows_person_0_0.csv"(HEADER=true, DELIM="|");
COPY PERSON_LIKES_COMMENT FROM "/data/ldbc10/person_likes_comment_0_0.csv"(HEADER=true, DELIM="|");
COPY PERSON_LIKES_POST FROM "/data/ldbc10/person_likes_post_0_0.csv"(HEADER=true, DELIM="|");
COPY PERSON_WORKAT_ORGANISATION FROM "/data/ldbc10/person_workAt_organisation_0_0.csv"(HEADER=true, DELIM="|");
COPY PLACE_ISPARTOF_PLACE FROM "/data/ldbc10/place_isPartOf_place_0_0.csv"(HEADER=true, DELIM="|");
COPY TAG_HASTYPE_TAGCLASS FROM "/data/ldbc10/tag_hasType_tagclass_0_0.csv"(HEADER=true, DELIM="|");
COPY TAGCLASS_ISSUBCLASSOF_TAGCLASS FROM "/data/ldbc10/tagclass_isSubclassOf_tagclass_0_0.csv"(HEADER=true, DELIM="|");
COPY PERSON_STUDYAT_ORGANISATION FROM "/data/ldbc10/person_studyAt_organisation_0_0.csv"(HEADER=true, DELIM="|");
