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

package com.alibaba.graphscope.cypher.integration.suite.movie;

import com.alibaba.graphscope.cypher.integration.suite.QueryContext;

import java.util.Arrays;
import java.util.List;

public class MovieQueries {
    public static QueryContext get_movie_query1_test() {
        String query =
                "MATCH (tom:Person) WHERE tom.name = \"Tom Hanks\" RETURN tom.born AS"
                        + " bornYear,tom.name AS personName;";
        List<String> expected =
                Arrays.asList("Record<{bornYear: 1956, personName: \"Tom Hanks\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query2_test() {
        String query =
                "MATCH (cloudAtlas:Movie {title: \"Cloud Atlas\"}) RETURN cloudAtlas.tagline AS"
                    + " tagline, cloudAtlas.released AS releasedYear,cloudAtlas.title AS title;";
        List<String> expected =
                Arrays.asList(
                        "Record<{tagline: \"Everything is connected\", releasedYear: 2012, title:"
                                + " \"Cloud Atlas\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query3_test() {
        String query =
                "MATCH (people:Person) RETURN people.name AS personName ORDER BY personName ASC"
                        + " LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{personName: \"Aaron Sorkin\"}>",
                        "Record<{personName: \"Al Pacino\"}>",
                        "Record<{personName: \"Annabella Sciorra\"}>",
                        "Record<{personName: \"Anthony Edwards\"}>",
                        "Record<{personName: \"Audrey Tautou\"}>",
                        "Record<{personName: \"Ben Miles\"}>",
                        "Record<{personName: \"Bill Paxton\"}>",
                        "Record<{personName: \"Bill Pullman\"}>",
                        "Record<{personName: \"Billy Crystal\"}>",
                        "Record<{personName: \"Bonnie Hunt\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query4_test() {
        String query =
                "MATCH (nineties:Movie) WHERE nineties.released >= 1990 AND nineties.released <"
                        + " 2000\n"
                        + "RETURN nineties.title AS ninetiesTitle ORDER BY ninetiesTitle DESC LIMIT"
                        + " 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{ninetiesTitle: \"You've Got Mail\"}>",
                        "Record<{ninetiesTitle: \"When Harry Met Sally\"}>",
                        "Record<{ninetiesTitle: \"What Dreams May Come\"}>",
                        "Record<{ninetiesTitle: \"Unforgiven\"}>",
                        "Record<{ninetiesTitle: \"Twister\"}>",
                        "Record<{ninetiesTitle: \"The Matrix\"}>",
                        "Record<{ninetiesTitle: \"The Green Mile\"}>",
                        "Record<{ninetiesTitle: \"The Devil's Advocate\"}>",
                        "Record<{ninetiesTitle: \"The Birdcage\"}>",
                        "Record<{ninetiesTitle: \"That Thing You Do\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query5_test() {
        String query =
                "MATCH (tom:Person {name: \"Tom Hanks\"})-[:ACTED_IN]->(tomHanksMovies)\n"
                        + "RETURN tom.born AS bornYear,\n"
                        + "tomHanksMovies.tagline AS movieTagline,\n"
                        + "tomHanksMovies.title AS movieTitle,\n"
                        + "tomHanksMovies.released AS releaseYear\n"
                        + "ORDER BY releaseYear DESC, movieTitle ASC LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{bornYear: 1956, movieTagline: \"Everything is connected\","
                                + " movieTitle: \"Cloud Atlas\", releaseYear: 2012}>",
                        "Record<{bornYear: 1956, movieTagline: \"A stiff drink. A little mascara. A"
                                + " lot of nerve. Who said they couldn't bring down the Soviet"
                                + " empire.\", movieTitle: \"Charlie Wilson's War\", releaseYear:"
                                + " 2007}>",
                        "Record<{bornYear: 1956, movieTagline: \"Break The Codes\", movieTitle:"
                                + " \"The Da Vinci Code\", releaseYear: 2006}>",
                        "Record<{bornYear: 1956, movieTagline: \"This Holiday Season... Believe\","
                                + " movieTitle: \"The Polar Express\", releaseYear: 2004}>",
                        "Record<{bornYear: 1956, movieTagline: \"At the edge of the world, his"
                            + " journey begins.\", movieTitle: \"Cast Away\", releaseYear: 2000}>",
                        "Record<{bornYear: 1956, movieTagline: \"Walk a mile you'll never"
                                + " forget.\", movieTitle: \"The Green Mile\", releaseYear: 1999}>",
                        "Record<{bornYear: 1956, movieTagline: \"At odds in life... in love"
                            + " on-line.\", movieTitle: \"You've Got Mail\", releaseYear: 1998}>",
                        "Record<{bornYear: 1956, movieTagline: \"In every life there comes a time"
                            + " when that thing you dream becomes that thing you do\", movieTitle:"
                            + " \"That Thing You Do\", releaseYear: 1996}>",
                        "Record<{bornYear: 1956, movieTagline: \"Houston, we have a problem.\","
                                + " movieTitle: \"Apollo 13\", releaseYear: 1995}>",
                        "Record<{bornYear: 1956, movieTagline: \"What if someone you never met,"
                            + " someone you never saw, someone you never knew was the only someone"
                            + " for you?\", movieTitle: \"Sleepless in Seattle\", releaseYear:"
                            + " 1993}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query6_test() {
        String query =
                "MATCH (cloudAtlas:Movie {title: \"Cloud Atlas\"})<-[:DIRECTED]-(directors)\n"
                    + "RETURN directors.name AS directorsName ORDER BY directorsName ASC LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{directorsName: \"Lana Wachowski\"}>",
                        "Record<{directorsName: \"Lilly Wachowski\"}>",
                        "Record<{directorsName: \"Tom Tykwer\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query7_test() {
        String query =
                "MATCH (tom:Person {name:\"Tom Hanks\"})-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors)\n"
                    + "RETURN m.title AS movieTitle, m.released AS releasedYear, coActors.name AS"
                    + " coActorName\n"
                    + "ORDER BY releasedYear DESC, movieTitle ASC, coActorName ASC LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{movieTitle: \"Cloud Atlas\", releasedYear: 2012, coActorName:"
                                + " \"Halle Berry\"}>",
                        "Record<{movieTitle: \"Cloud Atlas\", releasedYear: 2012, coActorName:"
                                + " \"Hugo Weaving\"}>",
                        "Record<{movieTitle: \"Cloud Atlas\", releasedYear: 2012, coActorName:"
                                + " \"Jim Broadbent\"}>",
                        "Record<{movieTitle: \"Cloud Atlas\", releasedYear: 2012, coActorName:"
                                + " \"Tom Hanks\"}>",
                        "Record<{movieTitle: \"Charlie Wilson's War\", releasedYear: 2007,"
                                + " coActorName: \"Julia Roberts\"}>",
                        "Record<{movieTitle: \"Charlie Wilson's War\", releasedYear: 2007,"
                                + " coActorName: \"Philip Seymour Hoffman\"}>",
                        "Record<{movieTitle: \"Charlie Wilson's War\", releasedYear: 2007,"
                                + " coActorName: \"Tom Hanks\"}>",
                        "Record<{movieTitle: \"The Da Vinci Code\", releasedYear: 2006,"
                                + " coActorName: \"Audrey Tautou\"}>",
                        "Record<{movieTitle: \"The Da Vinci Code\", releasedYear: 2006,"
                                + " coActorName: \"Ian McKellen\"}>",
                        "Record<{movieTitle: \"The Da Vinci Code\", releasedYear: 2006,"
                                + " coActorName: \"Paul Bettany\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query8_test() {
        String query =
                "MATCH (people:Person)-[relatedTo]-(:Movie {title: \"Cloud Atlas\"})\n"
                        + "RETURN personName, type(relatedTo), relatedTo";
        List<String> expected = Arrays.asList();
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query9_test() {
        String query =
                "MATCH (bacon:Person {name:\"Kevin Bacon\"})-[*1..3]-(hollywood)\n"
                        + "RETURN DISTINCT bacon, hollywood";
        List<String> expected = Arrays.asList();
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query10_test() {
        String query =
                "MATCH p=shortestPath(\n"
                        + "  (bacon:Person {name:\"Kevin Bacon\"})-[*]-(meg:Person {name:\"Meg"
                        + " Ryan\"})\n"
                        + ")\n"
                        + "RETURN p;";
        List<String> expected = Arrays.asList();
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query11_test() {
        String query =
                "MATCH (tom:Person {name: 'Tom Hanks'})-[r:ACTED_IN]->(movie:Movie)\n"
                        + "WITH movie.title as movieTitle, movie.released as movieReleased\n"
                        + "ORDER BY movieReleased DESC, movieTitle ASC LIMIT 10\n"
                        + "return movieTitle, movieReleased;";
        List<String> expected =
                Arrays.asList(
                        "Record<{movieTitle: \"Cloud Atlas\", movieReleased: 2012}>",
                        "Record<{movieTitle: \"Charlie Wilson's War\", movieReleased: 2007}>",
                        "Record<{movieTitle: \"The Da Vinci Code\", movieReleased: 2006}>",
                        "Record<{movieTitle: \"The Polar Express\", movieReleased: 2004}>",
                        "Record<{movieTitle: \"Cast Away\", movieReleased: 2000}>",
                        "Record<{movieTitle: \"The Green Mile\", movieReleased: 1999}>",
                        "Record<{movieTitle: \"You've Got Mail\", movieReleased: 1998}>",
                        "Record<{movieTitle: \"That Thing You Do\", movieReleased: 1996}>",
                        "Record<{movieTitle: \"Apollo 13\", movieReleased: 1995}>",
                        "Record<{movieTitle: \"Sleepless in Seattle\", movieReleased: 1993}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query12_test() {
        String query =
                "MATCH (tom:Person {name: 'Tom"
                    + " Hanks'})-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-(coActor:Person)\n"
                    + "WITH DISTINCT coActor.name AS coActorName ORDER BY coActorName ASC LIMIT 10"
                    + " return coActorName;";
        List<String> expected =
                Arrays.asList(
                        "Record<{coActorName: \"Audrey Tautou\"}>",
                        "Record<{coActorName: \"Bill Paxton\"}>",
                        "Record<{coActorName: \"Bill Pullman\"}>",
                        "Record<{coActorName: \"Bonnie Hunt\"}>",
                        "Record<{coActorName: \"Charlize Theron\"}>",
                        "Record<{coActorName: \"Dave Chappelle\"}>",
                        "Record<{coActorName: \"David Morse\"}>",
                        "Record<{coActorName: \"Ed Harris\"}>",
                        "Record<{coActorName: \"Gary Sinise\"}>",
                        "Record<{coActorName: \"Geena Davis\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query13_test() {
        String query =
                "MATCH (tom:Person {name: 'Tom"
                    + " Hanks'})-[:ACTED_IN]->(movie1:Movie)<-[:ACTED_IN]-(coActor:Person)-[:ACTED_IN]->(movie2:Movie)<-[:ACTED_IN]-(coCoActor:Person)\n"
                    + "WHERE tom <> coCoActor\n"
                    + "AND NOT (tom)-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-(coCoActor)\n"
                    + "RETURN coCoActor.name AS coCoActorName ORDER BY coCoActorName ASC LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{coCoActorName: \"Aaron Sorkin\"}>",
                        "Record<{coCoActorName: \"Al Pacino\"}>",
                        "Record<{coCoActorName: \"Anthony Edwards\"}>",
                        "Record<{coCoActorName: \"Anthony Edwards\"}>",
                        "Record<{coCoActorName: \"Anthony Edwards\"}>",
                        "Record<{coCoActorName: \"Ben Miles\"}>",
                        "Record<{coCoActorName: \"Billy Crystal\"}>",
                        "Record<{coCoActorName: \"Billy Crystal\"}>",
                        "Record<{coCoActorName: \"Billy Crystal\"}>",
                        "Record<{coCoActorName: \"Bruno Kirby\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query14_test() {
        String query =
                "MATCH (tom:Person {name: 'Tom"
                    + " Hanks'})-[:ACTED_IN]->(movie1:Movie)<-[:ACTED_IN]-(coActor:Person)-[:ACTED_IN]->(movie2:Movie)<-[:ACTED_IN]-(coCoActor:Person)\n"
                    + "WHERE tom <> coCoActor\n"
                    + "AND NOT (tom)-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-(coCoActor)\n"
                    + "RETURN coCoActor.name AS coCoActorName, count(coCoActor) AS frequency\n"
                    + "ORDER BY frequency DESC, coCoActorName ASC\n"
                    + "LIMIT 5;";
        List<String> expected =
                Arrays.asList(
                        "Record<{coCoActorName: \"Tom Cruise\", frequency: 5}>",
                        "Record<{coCoActorName: \"Zach Grenier\", frequency: 5}>",
                        "Record<{coCoActorName: \"Cuba Gooding Jr.\", frequency: 4}>",
                        "Record<{coCoActorName: \"Keanu Reeves\", frequency: 4}>",
                        "Record<{coCoActorName: \"Anthony Edwards\", frequency: 3}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query15_test() {
        String query =
                "MATCH (tom:Person {name: 'Tom"
                    + " Hanks'})-[:ACTED_IN]->(movie1:Movie)<-[:ACTED_IN]-(coActor:Person)-[:ACTED_IN]->(movie2:Movie)<-[:ACTED_IN]-(cruise:Person"
                    + " {name: 'Tom Cruise'})\n"
                    + "WHERE NOT (tom)-[:ACTED_IN]->(:Movie)<-[:ACTED_IN]-(cruise)\n"
                    + "RETURN tom.name AS actorName, movie1.title AS movie1Title, coActor.name AS"
                    + " coActorName, movie2.title AS movie2Title, cruise.name AS coCoActorName\n"
                    + "ORDER BY movie1Title ASC, movie2Title ASC LIMIT 10;";
        List<String> expected =
                Arrays.asList(
                        "Record<{actorName: \"Tom Hanks\", movie1Title: \"Apollo 13\", coActorName:"
                            + " \"Kevin Bacon\", movie2Title: \"A Few Good Men\", coCoActorName:"
                            + " \"Tom Cruise\"}>",
                        "Record<{actorName: \"Tom Hanks\", movie1Title: \"Joe Versus the Volcano\","
                            + " coActorName: \"Meg Ryan\", movie2Title: \"Top Gun\", coCoActorName:"
                            + " \"Tom Cruise\"}>",
                        "Record<{actorName: \"Tom Hanks\", movie1Title: \"Sleepless in Seattle\","
                            + " coActorName: \"Meg Ryan\", movie2Title: \"Top Gun\", coCoActorName:"
                            + " \"Tom Cruise\"}>",
                        "Record<{actorName: \"Tom Hanks\", movie1Title: \"The Green Mile\","
                                + " coActorName: \"Bonnie Hunt\", movie2Title: \"Jerry Maguire\","
                                + " coCoActorName: \"Tom Cruise\"}>",
                        "Record<{actorName: \"Tom Hanks\", movie1Title: \"You've Got Mail\","
                            + " coActorName: \"Meg Ryan\", movie2Title: \"Top Gun\", coCoActorName:"
                            + " \"Tom Cruise\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query16_test() {
        String query = "Match (n:Movie {id: 0}) Where n.title starts with 'The' Return n.title;";
        List<String> expected = Arrays.asList("Record<{title: \"The Matrix\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query17_test() {
        String query = "Match (n:Movie {id: 0}) Where n.title ends with 'Matrix' Return n.title;";
        List<String> expected = Arrays.asList("Record<{title: \"The Matrix\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query18_test() {
        String query = "Match (n:Movie {id: 0}) Where n.title contains 'The' Return n.title;";
        List<String> expected = Arrays.asList("Record<{title: \"The Matrix\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query19_test() {
        String query = "Match (n:Movie {id: 0}) Return [n.id, n.tagline] as value;";
        List<String> expected =
                Arrays.asList("Record<{value: [0, \"Welcome to the Real World\"]}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query20_test() {
        String query = "Match (n:Movie {id: 0}) Return {id: n.id, tagline: n.tagline} as value;";
        List<String> expected =
                Arrays.asList("Record<{value: {tagline: \"Welcome to the Real World\", id: 0}}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query21_test() {
        String query =
                "Match (tom:Person {name: 'Tom Hanks'})-[:ACTED_IN]->(movie1:Movie) Return"
                        + " count(tom, movie1);";
        List<String> expected = Arrays.asList("Record<{$f0: 12}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query22_test() {
        String query =
                "Match (tom:Person {name: 'Tom"
                        + " Hanks'})-[:ACTED_IN]->(movie1:Movie)<-[:ACTED_IN]-(p2:Person) Return"
                        + " count(distinct tom, p2);";
        List<String> expected = Arrays.asList("Record<{$f0: 35}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query23_test() {
        String query =
                "Match (tom:Person {name: 'Tom"
                    + " Hanks'})-[:ACTED_IN]->(movie1:Movie)<-[:ACTED_IN]-(p2:Person {name: \"Tom"
                    + " Hanks\"}) Return distinct tom.name, p2.name;";
        List<String> expected =
                Arrays.asList("Record<{name: \"Tom Hanks\", name0: \"Tom Hanks\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query24_test() {
        String query = "Match (n) Where labels(n)='Movie' Return distinct labels(n) as label;";
        List<String> expected = Arrays.asList("Record<{label: \"Movie\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query25_test() {
        String query =
                "Match (n)-[m]->(c) Where type(m)='ACTED_IN' Return distinct type(m) as type;";
        List<String> expected = Arrays.asList("Record<{type: \"ACTED_IN\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query26_test() {
        // property 'name' not exist in 'Movie', the count should be 0
        String query =
                "Match (a:Movie|Person) Return labels(a) as type, count(a.name) Order by type;";
        List<String> expected =
                Arrays.asList(
                        "Record<{type: \"Movie\", $f1: 0}>, Record<{type: \"Person\", $f1: 130}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query27_test() {
        String query =
                "Match (a:Person)-[b]->(c:Movie) Return distinct type(b) as type Order by type"
                        + " Limit 1;";
        List<String> expected = Arrays.asList("Record<{type: \"ACTED_IN\"}>");
        return new QueryContext(query, expected);
    }

    public static QueryContext get_movie_query28_test() {
        String query =
                "Match (a)-[:ACTED_IN]->(c) Return distinct labels(a) as typeA, labels(c) as"
                        + " typeC;";
        List<String> expected = Arrays.asList("Record<{typeA: \"Person\", typeC: \"Movie\"}>");
        return new QueryContext(query, expected);
    }
}
