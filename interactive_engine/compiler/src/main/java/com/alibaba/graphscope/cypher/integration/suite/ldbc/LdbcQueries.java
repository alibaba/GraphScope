/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.cypher.integration.suite.ldbc;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LdbcQueries {

    public static QueryContext get_ldbc_2_test() {
        String query =
                "MATCH (p:PERSON{id:"
                    + " 19791209300143})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(message : POST |"
                    + " COMMENT) \n"
                    + "WHERE message.creationDate < 20121128080000000 \n"
                    + "WITH \n"
                    + "\tfriend.id AS personId, \n"
                    + "\tfriend.firstName AS personFirstName, \n"
                    + "  friend.lastName AS personLastName, \n"
                    + "  message.id AS postOrCommentId, \n"
                    + "  message.creationDate AS postOrCommentCreationDate \n"
                    + "ORDER BY \n"
                    + "  postOrCommentCreationDate DESC, \n"
                    + "  postOrCommentId ASC \n"
                    + "LIMIT 20\n"
                    + "RETURN personId, personFirstName, personLastName, postOrCommentId";
        List<String> expected =
                Arrays.asList(
                        "Record<{personId: 24189255811566, personFirstName: \"The\","
                                + " personLastName: \"Kunda\", postOrCommentId: 1099511875186}>",
                        "Record<{personId: 30786325578747, personFirstName: \"Zhang\","
                                + " personLastName: \"Huang\", postOrCommentId: 1099511787223}>",
                        "Record<{personId: 8796093023000, personFirstName: \"Peng\","
                                + " personLastName: \"Zhang\", postOrCommentId: 1099511959866}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                                + " personLastName: \"Singh\", postOrCommentId: 1099511997952}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                                + " personLastName: \"Singh\", postOrCommentId: 1099511997953}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                                + " personLastName: \"Singh\", postOrCommentId: 1099511722622}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                                + " personLastName: \"Singh\", postOrCommentId: 1099511997861}>",
                        "Record<{personId: 4398046511596, personFirstName: \"Ge\", personLastName:"
                                + " \"Wei\", postOrCommentId: 1099511949726}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                                + " personLastName: \"Singh\", postOrCommentId: 1099511722621}>",
                        "Record<{personId: 8796093023000, personFirstName: \"Peng\","
                                + " personLastName: \"Zhang\", postOrCommentId: 1099511959863}>",
                        "Record<{personId: 4398046511596, personFirstName: \"Ge\", personLastName:"
                                + " \"Wei\", postOrCommentId: 1099511681330}>",
                        "Record<{personId: 4398046511596, personFirstName: \"Ge\", personLastName:"
                                + " \"Wei\", postOrCommentId: 1099511949721}>",
                        "Record<{personId: 8796093023000, personFirstName: \"Peng\","
                                + " personLastName: \"Zhang\", postOrCommentId: 1099511993242}>",
                        "Record<{personId: 8796093023000, personFirstName: \"Peng\","
                                + " personLastName: \"Zhang\", postOrCommentId: 1099511980256}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                                + " personLastName: \"Singh\", postOrCommentId: 1099511997871}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                                + " personLastName: \"Singh\", postOrCommentId: 1099511645534}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                                + " personLastName: \"Singh\", postOrCommentId: 1099511722160}>",
                        "Record<{personId: 30786325578747, personFirstName: \"Zhang\","
                                + " personLastName: \"Huang\", postOrCommentId: 1099511977072}>",
                        "Record<{personId: 24189255811566, personFirstName: \"The\","
                                + " personLastName: \"Kunda\", postOrCommentId: 1099511738988}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                                + " personLastName: \"Singh\", postOrCommentId: 1099511860894}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_ldbc_3_test() {
        String query =
                "MATCH (countryX:PLACE {name: 'Puerto_Rico'})<-[:ISLOCATEDIN]-(messageX: POST |"
                    + " COMMENT )-[:HASCREATOR]->(otherP:PERSON),\n"
                    + "    \t(countryY:PLACE {name:"
                    + " 'Republic_of_Macedonia'})<-[:ISLOCATEDIN]-(messageY: POST |"
                    + " COMMENT)-[:HASCREATOR]->(otherP:PERSON),\n"
                    + "    \t(otherP: PERSON)-[:ISLOCATEDIN]->(city:"
                    + " PLACE)-[:ISPARTOF]->(countryCity : PLACE),\n"
                    + "    \t(person:PERSON {id:15393162790207})-[:KNOWS*1..2]-(otherP: PERSON)\n"
                    + "WHERE messageX.creationDate >= 20101201080000000 and messageX.creationDate <"
                    + " 20101231080000000\n"
                    + "  AND messageY.creationDate >= 20101201080000000 and messageY.creationDate <"
                    + " 20101231080000000\n"
                    + "\tAND countryCity.name <> 'Puerto_Rico' AND countryCity.name <>"
                    + " 'Republic_of_Macedonia'\n"
                    + "WITH otherP, count(messageX) as xCount, count(messageY) as yCount\n"
                    + "RETURN otherP.id as id,\n"
                    + "\t\t\t otherP.firstName as firstName,\n"
                    + "\t\t\t otherP.lastName as lastName,\n"
                    + "\t\t\t xCount,\n"
                    + "\t\t\t yCount,\n"
                    + "\t\t\t xCount + yCount as total\n"
                    + "ORDER BY total DESC, id ASC\n"
                    + "Limit 20";
        return new QueryContext(query, Collections.emptyList());
    }

    public static QueryContext get_ldbc_4_test() {
        String query =
                "MATCH (person:PERSON {id:"
                    + " 10995116278874})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag:"
                    + " TAG)\n"
                    + "WITH DISTINCT tag, post\n"
                    + "WITH tag,\n"
                    + "     CASE\n"
                    + "       WHEN post.creationDate < 1340928000000  AND post.creationDate >="
                    + " 1338508800000 THEN 1\n"
                    + "       ELSE 0\n"
                    + "     END AS valid,\n"
                    + "     CASE\n"
                    + "       WHEN 1338508800000 > post.creationDate THEN 1\n"
                    + "       ELSE 0\n"
                    + "     END AS inValid\n"
                    + "WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount\n"
                    + "WHERE postCount>0 AND inValidPostCount=0\n"
                    + "\n"
                    + "RETURN tag.name AS tagName, postCount\n"
                    + "ORDER BY postCount DESC, tagName ASC\n"
                    + "LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{tagName: \"Norodom_Sihanouk\", postCount: 3}>",
                        "Record<{tagName: \"George_Clooney\", postCount: 1}>",
                        "Record<{tagName: \"Louis_Philippe_I\", postCount: 1}>");
        return new QueryContext(query, expected);
    }

    // minor diff with get_ldbc_4_test since in experiment store the date is in a different format
    // (e.g., 20120629020000000)
    public static QueryContext get_ldbc_4_test_exp() {
        String query =
                "MATCH (person:PERSON {id:"
                    + " 10995116278874})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag:"
                    + " TAG)\n"
                    + "WITH DISTINCT tag, post\n"
                    + "WITH tag,\n"
                    + "     CASE\n"
                    + "       WHEN post.creationDate < 20120629020000000  AND post.creationDate >="
                    + " 20120601000000000 THEN 1\n"
                    + "       ELSE 0\n"
                    + "     END AS valid,\n"
                    + "     CASE\n"
                    + "       WHEN 20120601000000000 > post.creationDate THEN 1\n"
                    + "       ELSE 0\n"
                    + "     END AS inValid\n"
                    + "WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount\n"
                    + "WHERE postCount>0 AND inValidPostCount=0\n"
                    + "\n"
                    + "RETURN tag.name AS tagName, postCount\n"
                    + "ORDER BY postCount DESC, tagName ASC\n"
                    + "LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{tagName: \"Norodom_Sihanouk\", postCount: 3}>",
                        "Record<{tagName: \"George_Clooney\", postCount: 1}>",
                        "Record<{tagName: \"Louis_Philippe_I\", postCount: 1}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_ldbc_6_test() {
        String query =
                "MATCH (person:PERSON"
                    + " {id:30786325579101})-[:KNOWS*1..3]-(other:PERSON)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag:TAG"
                    + " {name:'Shakira'}),\n"
                    + "      (post)-[:HASTAG]->(otherTag:TAG)\n"
                    + "WHERE otherTag <> tag\n"
                    + "RETURN otherTag.name as name, count(distinct post) as postCnt\n"
                    + "ORDER BY postCnt desc, name asc\n"
                    + "LIMIT 10";
        List<String> expected =
                Arrays.asList(
                        "Record<{name: \"David_Foster\", postCnt: 4}>",
                        "Record<{name: \"Muammar_Gaddafi\", postCnt: 2}>",
                        "Record<{name: \"Robert_John_Mutt_Lange\", postCnt: 2}>",
                        "Record<{name: \"Alfred_the_Great\", postCnt: 1}>",
                        "Record<{name: \"Andre_Agassi\", postCnt: 1}>",
                        "Record<{name: \"Andy_Roddick\", postCnt: 1}>",
                        "Record<{name: \"Bangladesh\", postCnt: 1}>",
                        "Record<{name: \"Benito_Mussolini\", postCnt: 1}>",
                        "Record<{name: \"Clark_Gable\", postCnt: 1}>",
                        "Record<{name: \"Condoleezza_Rice\", postCnt: 1}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_ldbc_7_test() {
        String query =
                "MATCH (person:PERSON {id: 26388279067534})<-[:HASCREATOR]-(message: POST |"
                        + " COMMENT)<-[like:LIKES]-(liker:PERSON)\n"
                        + "OPTIONAL MATCH (liker: PERSON)-[k:KNOWS]-(person: PERSON {id:"
                        + " 26388279067534})\n"
                        + "WITH liker, message, like.creationDate AS likeTime, person,\n"
                        + "  CASE\n"
                        + "      WHEN k is null THEN true\n"
                        + "      ELSE false\n"
                        + "     END AS isNew\n"
                        + "ORDER BY likeTime DESC, message.id ASC\n"
                        + "WITH liker, person, head(collect(message)) as message,"
                        + " head(collect(likeTime)) AS likeTime, isNew\n"
                        + "RETURN\n"
                        + "    liker.id AS personId,\n"
                        + "    liker.firstName AS personFirstName,\n"
                        + "    liker.lastName AS personLastName,\n"
                        + "    likeTime AS likeCreationDate,\n"
                        + "    message.id AS commentOrPostId,\n"
                        + "    message.content AS messageContent,\n"
                        + "    message.imageFile AS messageImageFile,\n"
                        + "    (likeTime - message.creationDate)/1000/60 AS minutesLatency,\n"
                        + "  \tisNew\n"
                        + "ORDER BY\n"
                        + "    likeCreationDate DESC,\n"
                        + "    personId ASC\n"
                        + "LIMIT 20;";
        List<String> expected =
                Arrays.asList(
                        "Record<{personId: 32985348834301, personFirstName: \"Anh\","
                            + " personLastName: \"Nguyen\", likeCreationDate: 1347061100109,"
                            + " commentOrPostId: 1030792374999, messageContent: \"About David"
                            + " Foster, llace's unfinished novel, About Paul McCartney,  as a solo"
                            + " artist and as aAbout \", messageImageFile: \"\", minutesLatency:"
                            + " 57622, isNew: FALSE}>",
                        "Record<{personId: 21990232556992, personFirstName: \"Shweta\","
                            + " personLastName: \"Kumar\", likeCreationDate: 1346511291019,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 5105, isNew: TRUE}>",
                        "Record<{personId: 15393162790476, personFirstName: \"K.\", personLastName:"
                            + " \"Rao\", likeCreationDate: 1346463964285, commentOrPostId:"
                            + " 1030792399080, messageContent: \"About I Only Have Eyes for You, ou"
                            + " is episode 19 of season two of Buffy the Vampire Sl\","
                            + " messageImageFile: \"\", minutesLatency: 4317, isNew: TRUE}>",
                        "Record<{personId: 10995116278184, personFirstName: \"Arjun\","
                            + " personLastName: \"Kumar\", likeCreationDate: 1346460875173,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 4265, isNew: FALSE}>",
                        "Record<{personId: 21990232556605, personFirstName: \"Arjun\","
                            + " personLastName: \"Sen\", likeCreationDate: 1346424083954,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 3652, isNew: TRUE}>",
                        "Record<{personId: 13194139534142, personFirstName: \"Rahul\","
                            + " personLastName: \"Reddy\", likeCreationDate: 1346410901910,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 3432, isNew: TRUE}>",
                        "Record<{personId: 8796093023493, personFirstName: \"Anupam\","
                            + " personLastName: \"Reddy\", likeCreationDate: 1346402462450,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 3292, isNew: TRUE}>",
                        "Record<{personId: 24189255811940, personFirstName: \"Arjun\","
                            + " personLastName: \"Khan\", likeCreationDate: 1346340478487,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 2259, isNew: TRUE}>",
                        "Record<{personId: 26388279067534, personFirstName: \"Emperor of Brazil\","
                            + " personLastName: \"Dom Pedro II\", likeCreationDate: 1346329544355,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 2076, isNew: TRUE}>",
                        "Record<{personId: 687, personFirstName: \"Deepak\", personLastName:"
                            + " \"Singh\", likeCreationDate: 1346323668418, commentOrPostId:"
                            + " 1030792399080, messageContent: \"About I Only Have Eyes for You, ou"
                            + " is episode 19 of season two of Buffy the Vampire Sl\","
                            + " messageImageFile: \"\", minutesLatency: 1978, isNew: TRUE}>",
                        "Record<{personId: 13194139533535, personFirstName: \"Shweta\","
                            + " personLastName: \"Singh\", likeCreationDate: 1346305628827,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 1678, isNew: TRUE}>",
                        "Record<{personId: 26388279067635, personFirstName: \"John\","
                                + " personLastName: \"Sheikh\", likeCreationDate: 1346293915386,"
                                + " commentOrPostId: 893353421832, messageContent: \"\","
                                + " messageImageFile: \"photo893353421832.jpg\", minutesLatency:"
                                + " 168543, isNew: FALSE}>",
                        "Record<{personId: 15393162790406, personFirstName: \"A.\", personLastName:"
                            + " \"Sharma\", likeCreationDate: 1346280033526, commentOrPostId:"
                            + " 1030792399080, messageContent: \"About I Only Have Eyes for You, ou"
                            + " is episode 19 of season two of Buffy the Vampire Sl\","
                            + " messageImageFile: \"\", minutesLatency: 1251, isNew: TRUE}>",
                        "Record<{personId: 8796093023060, personFirstName: \"Karim\","
                            + " personLastName: \"Akhmadiyeva\", likeCreationDate: 1346265643787,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 1011, isNew: TRUE}>",
                        "Record<{personId: 4398046511667, personFirstName: \"John\","
                            + " personLastName: \"Chopra\", likeCreationDate: 1346265192141,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 1004, isNew: TRUE}>",
                        "Record<{personId: 4398046512376, personFirstName: \"Jack\","
                            + " personLastName: \"Wilson\", likeCreationDate: 1346249759910,"
                            + " commentOrPostId: 1030792465816, messageContent: \"About Srivijaya,"
                            + " dated 16 June 682. The kingdom ceased to exiAbout Kingdom of"
                            + " Hanover,\", messageImageFile: \"\", minutesLatency: 1033, isNew:"
                            + " TRUE}>",
                        "Record<{personId: 2199023256816, personFirstName: \"K.\", personLastName:"
                            + " \"Bose\", likeCreationDate: 1346247410406, commentOrPostId:"
                            + " 1030792399080, messageContent: \"About I Only Have Eyes for You, ou"
                            + " is episode 19 of season two of Buffy the Vampire Sl\","
                            + " messageImageFile: \"\", minutesLatency: 707, isNew: FALSE}>",
                        "Record<{personId: 26388279067551, personFirstName: \"Anand\","
                            + " personLastName: \"Rao\", likeCreationDate: 1346235328898,"
                            + " commentOrPostId: 1030792399080, messageContent: \"About I Only Have"
                            + " Eyes for You, ou is episode 19 of season two of Buffy the Vampire"
                            + " Sl\", messageImageFile: \"\", minutesLatency: 506, isNew: TRUE}>",
                        "Record<{personId: 8796093022764, personFirstName: \"Zheng\","
                                + " personLastName: \"Xu\", likeCreationDate: 1346205231200,"
                                + " commentOrPostId: 962072804153, messageContent: \"About Bertolt"
                                + " Brecht,  – 14 August 1956) was a German poet, playwright, and"
                                + " theatre director. An influent\", messageImageFile: \"\","
                                + " minutesLatency: 84681, isNew: TRUE}>",
                        "Record<{personId: 28587302322631, personFirstName: \"David\","
                                + " personLastName: \"Fenter\", likeCreationDate: 1346201074886,"
                                + " commentOrPostId: 893353421832, messageContent: \"\","
                                + " messageImageFile: \"photo893353421832.jpg\", minutesLatency:"
                                + " 166995, isNew: FALSE}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_ldbc_8_test() {
        String query =
                "MATCH (person:PERSON {id:"
                    + " 2199023256816})<-[:HASCREATOR]-(message)<-[:REPLYOF]-(comment:COMMENT)-[:HASCREATOR]->(author:PERSON)\n"
                    + "RETURN \n"
                    + "\tauthor.id,\n"
                    + "\tauthor.firstName,\n"
                    + "\tauthor.lastName,\n"
                    + "\tcomment.creationDate as commentDate,\n"
                    + "\tcomment.id as commentId,\n"
                    + "\tcomment.content\n"
                    + "ORDER BY\n"
                    + "\tcommentDate desc,\n"
                    + "\tcommentId asc\n"
                    + "LIMIT 20";
        List<String> expected =
                Arrays.asList(
                        "Record<{id: 13194139533482, firstName: \"Ana Paula\", lastName: \"Silva\","
                                + " commentDate: 1347504375078, commentId: 1099511667820, content:"
                                + " \"About Heinz Guderian, aised and organized under his direction"
                                + " About Malacca Sul\"}>",
                        "Record<{id: 8796093022928, firstName: \"Hao\", lastName: \"Zhu\","
                            + " commentDate: 1347198063021, commentId: 1099511964827, content:"
                            + " \"About Nothing but the Beat, icki Minaj, Usher, Jennifer Hudson,"
                            + " Jessie J and Sia Furler\"}>",
                        "Record<{id: 10995116278796, firstName: \"Kenji\", lastName: \"Sakai\","
                            + " commentDate: 1347191906789, commentId: 1099511964825, content:"
                            + " \"About Humayun, to expand the Empire further, leaving a suAbout"
                            + " Philip K. Dick, r o\"}>",
                        "Record<{id: 30786325577752, firstName: \"Jie\", lastName: \"Yang\","
                                + " commentDate: 1347173707083, commentId: 1099511964826, content:"
                                + " \"no\"}>",
                        "Record<{id: 24189255812755, firstName: \"Paulo\", lastName: \"Santos\","
                                + " commentDate: 1347167706094, commentId: 1099511964828, content:"
                                + " \"good\"}>",
                        "Record<{id: 687, firstName: \"Deepak\", lastName: \"Singh\", commentDate:"
                                + " 1347101958087, commentId: 1030792351589, content: \"no"
                                + " way!\"}>",
                        "Record<{id: 2199023256586, firstName: \"Alfonso\", lastName: \"Elizalde\","
                            + " commentDate: 1347029913508, commentId: 1030792488768, content:"
                            + " \"About Humayun, ial legacy for his son, Akbar. His peaceful About"
                            + " Busta Rhymes, sta Rhy\"}>",
                        "Record<{id: 30786325578896, firstName: \"Yang\", lastName: \"Li\","
                                + " commentDate: 1347027425148, commentId: 1030792488774, content:"
                                + " \"roflol\"}>",
                        "Record<{id: 21990232555834, firstName: \"John\", lastName: \"Garcia\","
                                + " commentDate: 1347025241067, commentId: 1030792488763, content:"
                                + " \"no way!\"}>",
                        "Record<{id: 13194139534578, firstName: \"Kunal\", lastName: \"Sharma\","
                                + " commentDate: 1347020657245, commentId: 1030792488765, content:"
                                + " \"maybe\"}>",
                        "Record<{id: 15393162789932, firstName: \"Fali Sam\", lastName: \"Price\","
                                + " commentDate: 1347013079051, commentId: 1030792488767, content:"
                                + " \"roflol\"}>",
                        "Record<{id: 30786325579189, firstName: \"Cheh\", lastName: \"Yang\","
                                + " commentDate: 1346995568122, commentId: 1030792488759, content:"
                                + " \"yes\"}>",
                        "Record<{id: 555, firstName: \"Chen\", lastName: \"Yang\", commentDate:"
                            + " 1346986024535, commentId: 1030792488769, content: \"About Skin and"
                            + " Bones, Another Round, reprising the contribution he made to the"
                            + " original a\"}>",
                        "Record<{id: 13194139534382, firstName: \"A.\", lastName: \"Budjana\","
                                + " commentDate: 1346985914312, commentId: 1030792488758, content:"
                                + " \"duh\"}>",
                        "Record<{id: 8796093022290, firstName: \"Alexei\", lastName: \"Codreanu\","
                                + " commentDate: 1346966601712, commentId: 1030792488760, content:"
                                + " \"ok\"}>",
                        "Record<{id: 21990232555958, firstName: \"Ernest B\", lastName:"
                                + " \"Law-Yone\", commentDate: 1346962688132, commentId:"
                                + " 1030792488766, content: \"great\"}>",
                        "Record<{id: 26388279067760, firstName: \"Max\", lastName: \"Bauer\","
                                + " commentDate: 1346954071955, commentId: 1030792488761, content:"
                                + " \"thx\"}>",
                        "Record<{id: 10995116278300, firstName: \"Jie\", lastName: \"Li\","
                                + " commentDate: 1346953221751, commentId: 1030792488762, content:"
                                + " \"maybe\"}>",
                        "Record<{id: 10995116279093, firstName: \"Diem\", lastName: \"Nguyen\","
                                + " commentDate: 1346953186333, commentId: 1030792488764, content:"
                                + " \"thanks\"}>",
                        "Record<{id: 26388279066662, firstName: \"Alfonso\", lastName:"
                                + " \"Rodriguez\", commentDate: 1346935258972, commentId:"
                                + " 1030792487632, content: \"good\"}>");
        return new QueryContext(query, expected);
    }

    // minor diff with get_ldbc_8_test since in experiment store the date is in a different format
    // (e.g., 20120629020000000)
    public static QueryContext get_ldbc_8_test_exp() {
        String query =
                "MATCH (person:PERSON {id:"
                    + " 2199023256816})<-[:HASCREATOR]-(message)<-[:REPLYOF]-(comment:COMMENT)-[:HASCREATOR]->(author:PERSON)\n"
                    + "RETURN \n"
                    + "\tauthor.id,\n"
                    + "\tauthor.firstName,\n"
                    + "\tauthor.lastName,\n"
                    + "\tcomment.creationDate as commentDate,\n"
                    + "\tcomment.id as commentId,\n"
                    + "\tcomment.content\n"
                    + "ORDER BY\n"
                    + "\tcommentDate desc,\n"
                    + "\tcommentId asc\n"
                    + "LIMIT 20";
        List<String> expected =
                Arrays.asList(
                        "Record<{id: 13194139533482, firstName: \"Ana Paula\", lastName: \"Silva\","
                            + " commentDate: 20120913024615078, commentId: 1099511667820, content:"
                            + " \"About Heinz Guderian, aised and organized under his direction"
                            + " About Malacca Sul\"}>",
                        "Record<{id: 8796093022928, firstName: \"Hao\", lastName: \"Zhu\","
                            + " commentDate: 20120909134103021, commentId: 1099511964827, content:"
                            + " \"About Nothing but the Beat, icki Minaj, Usher, Jennifer Hudson,"
                            + " Jessie J and Sia Furler\"}>",
                        "Record<{id: 10995116278796, firstName: \"Kenji\", lastName: \"Sakai\","
                            + " commentDate: 20120909115826789, commentId: 1099511964825, content:"
                            + " \"About Humayun, to expand the Empire further, leaving a suAbout"
                            + " Philip K. Dick, r o\"}>",
                        "Record<{id: 30786325577752, firstName: \"Jie\", lastName: \"Yang\","
                            + " commentDate: 20120909065507083, commentId: 1099511964826, content:"
                            + " \"no\"}>",
                        "Record<{id: 24189255812755, firstName: \"Paulo\", lastName: \"Santos\","
                            + " commentDate: 20120909051506094, commentId: 1099511964828, content:"
                            + " \"good\"}>",
                        "Record<{id: 687, firstName: \"Deepak\", lastName: \"Singh\", commentDate:"
                                + " 20120908105918087, commentId: 1030792351589, content: \"no"
                                + " way!\"}>",
                        "Record<{id: 2199023256586, firstName: \"Alfonso\", lastName: \"Elizalde\","
                            + " commentDate: 20120907145833508, commentId: 1030792488768, content:"
                            + " \"About Humayun, ial legacy for his son, Akbar. His peaceful About"
                            + " Busta Rhymes, sta Rhy\"}>",
                        "Record<{id: 30786325578896, firstName: \"Yang\", lastName: \"Li\","
                            + " commentDate: 20120907141705148, commentId: 1030792488774, content:"
                            + " \"roflol\"}>",
                        "Record<{id: 21990232555834, firstName: \"John\", lastName: \"Garcia\","
                            + " commentDate: 20120907134041067, commentId: 1030792488763, content:"
                            + " \"no way!\"}>",
                        "Record<{id: 13194139534578, firstName: \"Kunal\", lastName: \"Sharma\","
                            + " commentDate: 20120907122417245, commentId: 1030792488765, content:"
                            + " \"maybe\"}>",
                        "Record<{id: 15393162789932, firstName: \"Fali Sam\", lastName: \"Price\","
                            + " commentDate: 20120907101759051, commentId: 1030792488767, content:"
                            + " \"roflol\"}>",
                        "Record<{id: 30786325579189, firstName: \"Cheh\", lastName: \"Yang\","
                            + " commentDate: 20120907052608122, commentId: 1030792488759, content:"
                            + " \"yes\"}>",
                        "Record<{id: 555, firstName: \"Chen\", lastName: \"Yang\", commentDate:"
                            + " 20120907024704535, commentId: 1030792488769, content: \"About Skin"
                            + " and Bones, Another Round, reprising the contribution he made to the"
                            + " original a\"}>",
                        "Record<{id: 13194139534382, firstName: \"A.\", lastName: \"Budjana\","
                            + " commentDate: 20120907024514312, commentId: 1030792488758, content:"
                            + " \"duh\"}>",
                        "Record<{id: 8796093022290, firstName: \"Alexei\", lastName: \"Codreanu\","
                            + " commentDate: 20120906212321712, commentId: 1030792488760, content:"
                            + " \"ok\"}>",
                        "Record<{id: 21990232555958, firstName: \"Ernest B\", lastName:"
                                + " \"Law-Yone\", commentDate: 20120906201808132, commentId:"
                                + " 1030792488766, content: \"great\"}>",
                        "Record<{id: 26388279067760, firstName: \"Max\", lastName: \"Bauer\","
                            + " commentDate: 20120906175431955, commentId: 1030792488761, content:"
                            + " \"thx\"}>",
                        "Record<{id: 10995116278300, firstName: \"Jie\", lastName: \"Li\","
                            + " commentDate: 20120906174021751, commentId: 1030792488762, content:"
                            + " \"maybe\"}>",
                        "Record<{id: 10995116279093, firstName: \"Diem\", lastName: \"Nguyen\","
                            + " commentDate: 20120906173946333, commentId: 1030792488764, content:"
                            + " \"thanks\"}>",
                        "Record<{id: 26388279066662, firstName: \"Alfonso\", lastName:"
                                + " \"Rodriguez\", commentDate: 20120906124058972, commentId:"
                                + " 1030792487632, content: \"good\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_ldbc_10_test() {
        String query =
                "MATCH (person:PERSON {id: 30786325579101})-[:KNOWS*2..3]-(friend:"
                    + " PERSON)-[:ISLOCATEDIN]->(city:PLACE)\n"
                    + "WHERE NOT friend=person AND NOT (friend:PERSON)-[:KNOWS]-(person :PERSON"
                    + " {id: 30786325579101})\n"
                    + "WITH person, city, friend, friend.birthday as birthday\n"
                    + "WHERE  (birthday.month=7 AND birthday.day>=21) OR\n"
                    + "        (birthday.month=8 AND birthday.day<22)\n"
                    + "WITH DISTINCT friend, city, person\n"
                    + "\n"
                    + "OPTIONAL MATCH (friend : PERSON)<-[:HASCREATOR]-(post:POST)\n"
                    + "WITH friend, city, person, count(post) as postCount\n"
                    + "\n"
                    + "OPTIONAL MATCH"
                    + " (friend)<-[:HASCREATOR]-(post1:POST)-[:HASTAG]->(tag:TAG)<-[:HASINTEREST]-(person:"
                    + " PERSON {id: 30786325579101})\n"
                    + "WITH friend, city, postCount, count(post1) as commonPostCount\n"
                    + "\n"
                    + "RETURN friend.id AS personId,\n"
                    + "       friend.firstName AS personFirstName,\n"
                    + "       friend.lastName AS personLastName,\n"
                    + "       commonPostCount - (postCount - commonPostCount) AS"
                    + " commonInterestScore,\n"
                    + "       friend.gender AS personGender,\n"
                    + "       city.name AS personCityName\n"
                    + "ORDER BY commonInterestScore DESC, personId ASC\n"
                    + "LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{personId: 10995116278223, personFirstName: \"Guy\","
                            + " personLastName: \"Akongo\", commonInterestScore: 0, personGender:"
                            + " \"female\", personCityName: \"Mokolo\"}>",
                        "Record<{personId: 19791209301505, personFirstName: \"Henry\","
                            + " personLastName: \"Smith\", commonInterestScore: 0, personGender:"
                            + " \"male\", personCityName: \"Coventry\"}>",
                        "Record<{personId: 32985348833798, personFirstName: \"Otto\","
                                + " personLastName: \"Kerndlova\", commonInterestScore: 0,"
                                + " personGender: \"male\", personCityName: \"České_Budějovice\"}>",
                        "Record<{personId: 6597069767635, personFirstName: \"Thomas Ilenda\","
                            + " personLastName: \"Lita\", commonInterestScore: -1, personGender:"
                            + " \"female\", personCityName: \"Bandundu\"}>",
                        "Record<{personId: 19791209300656, personFirstName: \"Francis\","
                            + " personLastName: \"Aquino\", commonInterestScore: -1, personGender:"
                            + " \"male\", personCityName: \"Iligan\"}>",
                        "Record<{personId: 30786325578904, personFirstName: \"Giuseppe\","
                            + " personLastName: \"Donati\", commonInterestScore: -2, personGender:"
                            + " \"male\", personCityName: \"Turin\"}>",
                        "Record<{personId: 24189255811227, personFirstName: \"Bacary\","
                            + " personLastName: \"Diop\", commonInterestScore: -4, personGender:"
                            + " \"male\", personCityName: \"Diourbel\"}>",
                        "Record<{personId: 17592186045405, personFirstName: \"Wei\","
                                + " personLastName: \"Li\", commonInterestScore: -5, personGender:"
                                + " \"female\", personCityName: \"Luoyang\"}>",
                        "Record<{personId: 13194139533984, personFirstName: \"Annemarie\","
                                + " personLastName: \"Bos\", commonInterestScore: -8, personGender:"
                                + " \"female\", personCityName: \"Utrecht\"}>",
                        "Record<{personId: 26388279068275, personFirstName: \"Jean\","
                            + " personLastName: \"Berty\", commonInterestScore: -13, personGender:"
                            + " \"male\", personCityName: \"Buea\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_ldbc_12() {
        String query =
                "MATCH (person:PERSON {id:"
                    + " 19791209300143})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(comment:COMMENT)-[:REPLYOF]->(:POST)-[:HASTAG]->(tag:TAG)-[:HASTYPE]->(:TAGCLASS)-[:ISSUBCLASSOF*0..5]->(baseTagClass:TAGCLASS"
                    + " {name: 'BasketballPlayer'})\n"
                    + "RETURN\n"
                    + "  friend.id AS personId,\n"
                    + "  friend.firstName AS personFirstName,\n"
                    + "  friend.lastName AS personLastName,\n"
                    + "  collect(DISTINCT tag.name) AS tagNames,\n"
                    + "  count(DISTINCT comment) AS replyCount\n"
                    + "ORDER BY\n"
                    + "  replyCount DESC,\n"
                    + "  personId ASC\n"
                    + "LIMIT 20";
        List<String> expected =
                Arrays.asList(
                        "Record<{personId: 8796093023000, personFirstName: \"Peng\","
                                + " personLastName: \"Zhang\", tagNames: [\"Michael_Jordan\"],"
                                + " replyCount: 4}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_st_path() {
        String query =
                "Match (a:PERSON {id: 2199023256684})-[c:KNOWS*5..6]->(b:PERSON {id:"
                        + " 8796093023060}) Return length(c) as len;";
        List<String> expected = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            expected.add("Record<{len: 5}>");
        }
        return new QueryContext(query, expected);
    }
}
