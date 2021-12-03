/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.dataload.ldbc.jsongen;

import com.alibaba.maxgraph.dataload.databuild.FileColumnMapping;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws JsonProcessingException {
        Map<String, FileColumnMapping> m = new HashMap<>();

        m.put("comment_0_0.csv", new Comment().toFileColumnMapping());
        m.put("forum_0_0.csv", new Forum().toFileColumnMapping());
        m.put("person_0_0.csv", new Person().toFileColumnMapping());
        m.put("post_0_0.csv", new Post().toFileColumnMapping());
        m.put("place_0_0.csv", new Place().toFileColumnMapping());
        m.put("tag_0_0.csv", new Tag().toFileColumnMapping());
        m.put("tagclass_0_0.csv", new Tagclass().toFileColumnMapping());
        m.put("organisation_0_0.csv", new Organisation().toFileColumnMapping());

        m.put(
                "comment_hasCreator_person_0_0.csv",
                new CommentHasCreatorPerson().toFileColumnMapping());
        m.put("comment_hasTag_tag_0_0.csv", new CommentHasTagTag().toFileColumnMapping());
        m.put(
                "comment_isLocatedIn_place_0_0.csv",
                new CommentIsLocatedInPlace().toFileColumnMapping());
        m.put("comment_replyOf_comment_0_0.csv", new CommentReplyOfComment().toFileColumnMapping());
        m.put("comment_replyOf_post_0_0.csv", new CommentReplyOfPost().toFileColumnMapping());
        m.put("forum_containerOf_post_0_0.csv", new ForumContainerOfPost().toFileColumnMapping());
        m.put("forum_hasMember_person_0_0.csv", new ForumHasMemberPerson().toFileColumnMapping());
        m.put(
                "forum_hasModerator_person_0_0.csv",
                new ForumHasModeratorPerson().toFileColumnMapping());
        m.put("forum_hasTag_tag_0_0.csv", new ForumHasTagTag().toFileColumnMapping());
        m.put("person_hasInterest_tag_0_0.csv", new PersonHasInterestTag().toFileColumnMapping());
        m.put(
                "person_isLocatedIn_place_0_0.csv",
                new PersonIsLocatedInPlace().toFileColumnMapping());
        m.put("person_knows_person_0_0.csv", new PersonKnowsPerson().toFileColumnMapping());
        m.put("person_likes_comment_0_0.csv", new PersonLikesComment().toFileColumnMapping());
        m.put("person_likes_post_0_0.csv", new PersonLikesPost().toFileColumnMapping());
        m.put(
                "person_studyAt_organisation_0_0.csv",
                new PersonStudyAtOrganisation().toFileColumnMapping());
        m.put(
                "person_workAt_organisation_0_0.csv",
                new PersonWorkAtOrganisation().toFileColumnMapping());
        m.put("post_hasCreator_person_0_0.csv", new PostHasCreatorPerson().toFileColumnMapping());
        m.put("post_hasTag_tag_0_0.csv", new PostHasTagTag().toFileColumnMapping());
        m.put("post_isLocatedIn_place_0_0.csv", new PostIsLocatedInPlace().toFileColumnMapping());
        m.put(
                "organisation_isLocatedIn_place_0_0.csv",
                new OrganisationIsLocatedInPlace().toFileColumnMapping());
        m.put("place_isPartOf_place_0_0.csv", new PlaceIsPartOfPlace().toFileColumnMapping());
        m.put("tag_hasType_tagclass_0_0.csv", new TagHasTypeTagclass().toFileColumnMapping());
        m.put(
                "tagclass_isSubclassOf_tagclass_0_0.csv",
                new TagclassIsSubclassOfTagclass().toFileColumnMapping());

        System.out.println(new ObjectMapper().writeValueAsString(m));
    }
}
