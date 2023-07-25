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
                "MATCH (countryX:PLACE {name:"
                    + " 'Puerto_Rico'})<-[:ISLOCATEDIN]-(messageX)-[:HASCREATOR]->(otherP:PERSON),\n"
                    + "    \t(countryY:PLACE {name:"
                    + " 'Republic_of_Macedonia'})<-[:ISLOCATEDIN]-(messageY)-[:HASCREATOR]->(otherP:PERSON),\n"
                    + "    \t(otherP)-[:ISLOCATEDIN]->(city)-[:ISPARTOF]->(countryCity),\n"
                    + "    \t(person:PERSON {id:15393162790207})-[:KNOWS*1..2]-(otherP)\n"
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
}
