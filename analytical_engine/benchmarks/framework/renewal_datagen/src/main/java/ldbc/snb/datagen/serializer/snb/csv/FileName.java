package ldbc.snb.datagen.serializer.snb.csv;

public enum FileName {

    // static
    TAG("tag"),
    TAG_HAS_TYPE_TAGCLASS("tag_hasType_tagclass"),
    TAGCLASS("tagclass"),
    TAGCLASS_IS_SUBCLASS_OF_TAGCLASS("tagclass_isSubclassOf_tagclass"),
    PLACE("place"),
    PLACE_IS_PART_OF_PLACE("place_isPartOf_place"),
    ORGANISATION("organisation"),
    ORGANISATION_IS_LOCATED_IN_PLACE("organisation_isLocatedIn_place"),

    // dynamic activity
    FORUM("forum"),
    FORUM_CONTAINEROF_POST("forum_containerOf_post"),
    FORUM_HASMEMBER_PERSON("forum_hasMember_person"),
    FORUM_HASMODERATOR_PERSON("forum_hasModerator_person"),
    FORUM_HASTAG_TAG("forum_hasTag_tag"),
    PERSON_LIKES_POST("person_likes_post"),
    PERSON_LIKES_COMMENT("person_likes_comment"),
    POST("post"),
    POST_HASCREATOR_PERSON("post_hasCreator_person"),
    POST_HASTAG_TAG("post_hasTag_tag"),
    POST_ISLOCATEDIN_PLACE("post_isLocatedIn_place"),
    COMMENT("comment"),
    COMMENT_HASCREATOR_PERSON("comment_hasCreator_person"),
    COMMENT_HASTAG_TAG("comment_hasTag_tag"),
    COMMENT_ISLOCATEDIN_PLACE("comment_isLocatedIn_place"),
    COMMENT_REPLYOF_POST("comment_replyOf_post"),
    COMMENT_REPLYOF_COMMENT("comment_replyOf_comment"),

    // dynamic person
    PERSON("person"),
    PERSON_SPEAKS_LANGUAGE("person_speaks_language"),
    PERSON_HAS_EMAIL("person_email_emailaddress"),
    PERSON_LOCATED_IN_PLACE("person_isLocatedIn_place"),
    PERSON_HAS_INTEREST_TAG("person_hasInterest_tag"),
    PERSON_WORK_AT("person_workAt_organisation"),
    PERSON_STUDY_AT("person_studyAt_organisation"),
    PERSON_KNOWS_PERSON("person_knows_person"),

    // single file for each
    SOCIAL_NETWORK_STATIC("social_network_static"),
    SOCIAL_NETWORK_ACTIVITY("social_network_activity"),
    SOCIAL_NETWORK_PERSON("social_network_person"),
    ;

    private final String name;

    FileName(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

}
