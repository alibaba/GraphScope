package ldbc.snb.datagen.test;

import ldbc.snb.datagen.LdbcDatagen;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.test.csv.ColumnSet;
import ldbc.snb.datagen.test.csv.ExistsCheck;
import ldbc.snb.datagen.test.csv.FileChecker;
import ldbc.snb.datagen.test.csv.LongCheck;
import ldbc.snb.datagen.test.csv.LongPairCheck;
import ldbc.snb.datagen.test.csv.LongParser;
import ldbc.snb.datagen.test.csv.NumericCheck;
import ldbc.snb.datagen.test.csv.NumericPairCheck;
import ldbc.snb.datagen.test.csv.PairUniquenessCheck;
import ldbc.snb.datagen.test.csv.StringLengthCheck;
import ldbc.snb.datagen.test.csv.StringParser;
import ldbc.snb.datagen.test.csv.UniquenessCheck;
import ldbc.snb.datagen.util.ConfigParser;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class LdbcDatagenTest {

    private final String dir = "./test_data/social_network";
    private final String sdir = "./test_data/substitution_parameters";

    @BeforeClass
    public static void generateData() throws Exception {
        Configuration conf = ConfigParser.initialize();
        ConfigParser.readConfig(conf, "./test_params.ini");
        ConfigParser.readConfig(conf, LdbcDatagen.class.getResourceAsStream("/params_default.ini"));
        try {
            LdbcDatagen.prepareConfiguration(conf);
            LdbcDatagen.initializeContext(conf);
            LdbcDatagen datagen = new LdbcDatagen();
            datagen.runGenerateJob(conf);
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void personTest() {
        testIdUniqueness(dir+"/dynamic/person_0_0.csv", 0);
        testStringLength(dir+"/dynamic/person_0_0.csv", 1, 40);
        testStringLength(dir+"/dynamic/person_0_0.csv", 2, 40);
        testStringLength(dir+"/dynamic/person_0_0.csv", 3, 40);
        testStringLength(dir+"/dynamic/person_0_0.csv", 6, 40);
        testStringLength(dir+"/dynamic/person_0_0.csv", 7, 40);
        assertTrue("Everything ok",true);
    }

    @Test
    public void postTest() {
        testIdUniqueness(dir+"/dynamic/post_0_0.csv", 0);
        testLongBetween(dir+"/dynamic/post_0_0.csv",7,0,2001);
        testStringLength(dir+"/dynamic/post_0_0.csv", 1, 40);
        testStringLength(dir+"/dynamic/post_0_0.csv", 3, 40);
        testStringLength(dir+"/dynamic/post_0_0.csv", 4, 40);
        testStringLength(dir+"/dynamic/post_0_0.csv", 5, 40);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumTest() {
        testIdUniqueness(dir+"/dynamic/forum_0_0.csv", 0);
        testStringLength(dir+"/dynamic/forum_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentTest() {
        testIdUniqueness(dir+"/dynamic/comment_0_0.csv", 0);
        testLongBetween(dir+"/dynamic/comment_0_0.csv",5,0,2001);
        testStringLength(dir+"/dynamic/comment_0_0.csv", 2, 40);
        testStringLength(dir+"/dynamic/comment_0_0.csv", 3, 40);
        assertTrue("Everything ok",true);
    }

    @Test
    public void organisationTest() {
        testIdUniqueness(dir+"/static/organisation_0_0.csv", 0);
        testStringLength(dir+"/static/organisation_0_0.csv", 2, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void placeTest() {
        testIdUniqueness(dir+"/static/place_0_0.csv", 0);
        testStringLength(dir+"/static/place_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void tagTest() {
        testIdUniqueness(dir+"/static/tag_0_0.csv", 0);
        testStringLength(dir+"/static/tag_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void tagclassTest() {
        testIdUniqueness(dir+"/static/tagclass_0_0.csv", 0);
        testStringLength(dir+"/static/tagclass_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personKnowsPersonTest() {
        testPairUniquenessPlusExistance(dir+"/dynamic/person_knows_person_0_0.csv",0,1,dir+"/dynamic/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void organisationIsLocatedInPlaceTest() {
        testPairUniquenessPlusExistance(dir+"/static/organisation_isLocatedIn_place_0_0.csv",0,1,dir+"/static/organisation_0_0.csv",0,dir+"/static/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void placeIsPartOfPlaceTest() {
        testPairUniquenessPlusExistance(dir+"/static/place_isPartOf_place_0_0.csv",0,1,dir+"/static/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void tagClassIsSubclassOfTest() {
        testPairUniquenessPlusExistance(dir+"/static/tagclass_isSubclassOf_tagclass_0_0.csv",0,1,dir+"/static/tagclass_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void tagHasTypeTagclassCheck() {
        testPairUniquenessPlusExistance(dir+"/static/tag_hasType_tagclass_0_0.csv",0,1,dir+"/static/tag_0_0.csv",0,dir+"/static/tagclass_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personStudyAtOrganisationCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/person_studyAt_organisation_0_0.csv",0,1,dir+"/dynamic/person_0_0.csv",0,dir+"/static/organisation_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personWorkAtOrganisationCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/person_workAt_organisation_0_0.csv",0,1,dir+"/dynamic/person_0_0.csv",0,dir+"/static/organisation_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personHasInterestTagCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/person_hasInterest_tag_0_0.csv",0,1,dir+"/dynamic/person_0_0.csv",0,dir+"/static/tag_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/person_isLocatedIn_place_0_0.csv",0,1,dir+"/dynamic/person_0_0.csv",0,dir+"/static/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumHasTagCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/forum_hasTag_tag_0_0.csv",0,1,dir+"/dynamic/forum_0_0.csv",0,dir+"/static/tag_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumHasModeratorPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/forum_hasModerator_person_0_0.csv",0,1,dir+"/dynamic/forum_0_0.csv",0,dir+"/dynamic/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumHasMemberPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/forum_hasMember_person_0_0.csv",0,1,dir+"/dynamic/forum_0_0.csv",0,dir+"/dynamic/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void forumContainerOfPostCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/forum_containerOf_post_0_0.csv",0,1,dir+"/dynamic/forum_0_0.csv",0,dir+"/dynamic/post_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentHasCreatorPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/comment_hasCreator_person_0_0.csv",0,1,dir+"/dynamic/comment_0_0.csv",0,dir+"/dynamic/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentHasTagTagCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/comment_hasTag_tag_0_0.csv",0,1,dir+"/dynamic/comment_0_0.csv",0,dir+"/static/tag_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/comment_isLocatedIn_place_0_0.csv",0,1,dir+"/dynamic/comment_0_0.csv",0,dir+"/static/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentReplyOfCommentCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/comment_replyOf_comment_0_0.csv",0,1,dir+"/dynamic/comment_0_0.csv",0,dir+"/dynamic/comment_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void commentReplyOfPostCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/comment_replyOf_post_0_0.csv",0,1,dir+"/dynamic/comment_0_0.csv",0,dir+"/dynamic/post_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void postHasCreatorPersonCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/post_hasCreator_person_0_0.csv",0,1,dir+"/dynamic/post_0_0.csv",0,dir+"/dynamic/person_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void postIsLocatedInPlaceCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/post_isLocatedIn_place_0_0.csv",0,1,dir+"/dynamic/post_0_0.csv",0,dir+"/static/place_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personLikesCommentCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/person_likes_comment_0_0.csv",0,1,dir+"/dynamic/person_0_0.csv",0,dir+"/dynamic/comment_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personLikesPostCheck() {
        testPairUniquenessPlusExistance(dir+"/dynamic/person_likes_post_0_0.csv",0,1,dir+"/dynamic/person_0_0.csv",0,dir+"/dynamic/post_0_0.csv",0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void personEmailAddressCheck() {
        testIdExistance(dir+"/dynamic/person_0_0.csv",0,dir+"/dynamic/person_email_emailaddress_0_0.csv",0);
        testStringLength(dir+"/dynamic/person_email_emailaddress_0_0.csv", 1, 256);
        assertTrue("Everything ok",true);
    }

    // test update stream  time consistency
    @Test
    public void updateStreamForumsConsistencyCheck() {
        testLongPair(dir+"/updateStream_0_0_forum.csv",0,1,NumericPairCheck.NumericCheckType.GE, -10000,0);
        assertTrue("Everything ok",true);
    }

    @Test
    public void queryParamsTest() {
        //Creating person id check
        LongParser parser = new LongParser();
        ColumnSet<Long> persons = new ColumnSet<>(parser,new File(dir+"/dynamic/person_0_0.csv"),0,1);
        List<ColumnSet<Long>> personsRef = new ArrayList<>();
        personsRef.add(persons);
        List<Integer> personIndex = new ArrayList<>();
        personIndex.add(0);
        ExistsCheck<Long> existsPersonCheck = new ExistsCheck<>(parser,personIndex, personsRef);

        //Creating name check
        StringParser strParser = new StringParser();
        ColumnSet<String> names = new ColumnSet<>(strParser,new File(dir+"/dynamic/person_0_0.csv"),1,1);
        List<ColumnSet<String>> namesRef = new ArrayList<>();
        namesRef.add(names);
        List<Integer> namesIndex = new ArrayList<>();
        namesIndex.add(1);
        ExistsCheck<String> existsNameCheck = new ExistsCheck<>(strParser,namesIndex, namesRef);



        FileChecker fileChecker = new FileChecker(sdir+"/interactive_1_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(existsNameCheck);
        assertTrue("ERROR PASSING TEST QUERY 1 PERSON AND NAME EXISTS ",fileChecker.run(1));

        //Crating date interval check
        fileChecker = new FileChecker(sdir+"/interactive_2_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 2 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_2_param.txt",1, Dictionaries.dates.getStartDateTime());

        //Creating country check
        ColumnSet<String> places = new ColumnSet<>(strParser,new File(dir+"/static/place_0_0.csv"),1,1);
        List<ColumnSet<String>> placesRef = new ArrayList<>();
        placesRef.add(places);
        List<Integer> countriesIndex = new ArrayList<>();
        countriesIndex.add(3);
        countriesIndex.add(4);
        ExistsCheck<String> countryExists = new ExistsCheck<>(strParser,countriesIndex, placesRef);

        //Date duration check
        //DateDurationCheck dateDurationCheck = new DateDurationCheck("Date duration check",1,2,Dictionaries.dates
         //       .getStartDateTime(), Dictionaries.dates.getEndDateTime());

        fileChecker = new FileChecker(sdir+"/interactive_3_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(countryExists);
        assertTrue("ERROR PASSING TEST QUERY 3 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_3_param.txt",1, Dictionaries.dates.getStartDateTime());

        fileChecker = new FileChecker(sdir+"/interactive_4_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 4 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_4_param.txt",1, Dictionaries.dates.getStartDateTime());

        fileChecker = new FileChecker(sdir+"/interactive_5_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 5 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_5_param.txt",1, Dictionaries.dates.getStartDateTime());

        //Creating tag check
        ColumnSet<String> tags = new ColumnSet<>(strParser,new File(dir+"/static/tag_0_0.csv"),1,1);
        List<ColumnSet<String>> tagsRef = new ArrayList<>();
        tagsRef.add(tags);
        List<Integer> tagsIndex = new ArrayList<>();
        tagsIndex.add(1);
        ExistsCheck<String> tagExists = new ExistsCheck<>(strParser,tagsIndex, tagsRef);

        fileChecker = new FileChecker(sdir+"/interactive_6_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(tagExists);
        assertTrue("ERROR PASSING TEST QUERY 6 PERSON EXISTS ",fileChecker.run(1));

        fileChecker = new FileChecker(sdir+"/interactive_7_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 7 PERSON EXISTS ",fileChecker.run(1));

        fileChecker = new FileChecker(sdir+"/interactive_8_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 8 PERSON EXISTS ",fileChecker.run(1));

        fileChecker = new FileChecker(sdir+"/interactive_9_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 9 PERSON EXISTS ",fileChecker.run(1));
        testLongGE(sdir+"/interactive_9_param.txt",1, Dictionaries.dates.getStartDateTime());

        fileChecker = new FileChecker(sdir+"/interactive_10_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 10 PERSON EXISTS ",fileChecker.run(1));
        testLongBetween(sdir+"/interactive_10_param.txt",1, 1, 13);

        //Creating country check
        countriesIndex.clear();
        countriesIndex.add(1);
        countryExists = new ExistsCheck<>(strParser,countriesIndex, placesRef);

        fileChecker = new FileChecker(sdir+"/interactive_11_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(countryExists);
        assertTrue("ERROR PASSING TEST QUERY 11 PERSON EXISTS ",fileChecker.run(1));

        //Creating tagClass check
        ColumnSet<String> tagClass = new ColumnSet<>(strParser,new File(dir+"/static/tagclass_0_0.csv"),1,1);
        List<ColumnSet<String>> tagClassRef = new ArrayList<>();
        tagClassRef.add(tagClass);
        List<Integer> tagClassIndex = new ArrayList<>();
        tagClassIndex.add(1);
        ExistsCheck<String> tagClassExists = new ExistsCheck<>(strParser,tagClassIndex, tagClassRef);

        fileChecker = new FileChecker(sdir+"/interactive_12_param.txt");
        fileChecker.addCheck(existsPersonCheck);
        fileChecker.addCheck(tagClassExists);
        assertTrue("ERROR PASSING TEST QUERY 12 PERSON EXISTS ",fileChecker.run(1));

        personIndex.add(1);
        ExistsCheck<Long> exists2PersonCheck = new ExistsCheck<>(parser,personIndex, personsRef);

        fileChecker = new FileChecker(sdir+"/interactive_13_param.txt");
        fileChecker.addCheck(exists2PersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 13 PERSON EXISTS ",fileChecker.run(1));

        fileChecker = new FileChecker(sdir+"/interactive_14_param.txt");
        fileChecker.addCheck(exists2PersonCheck);
        assertTrue("ERROR PASSING TEST QUERY 14 PERSON EXISTS ",fileChecker.run(1));

    }

    public void testLongPair(String fileName, Integer columnA, Integer columnB, NumericPairCheck.NumericCheckType type, long offsetA, long offsetB) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongPairCheck check = new LongPairCheck(parser, " Long check ", columnA, columnB, type, offsetA, offsetB);
        fileChecker.addCheck(check);
        assertTrue("ERROR PASSING TEST LONG PAIR FOR FILE "+fileName,fileChecker.run(0));
    }

    public void testIdUniqueness(String fileName, int column) {
        FileChecker fileChecker = new FileChecker(fileName);
        UniquenessCheck check = new UniquenessCheck(column);
        fileChecker.addCheck(check);
        assertTrue("ERROR PASSING TEST ID UNIQUENESS FOR FILE "+fileName,fileChecker.run(1));
    }

    public void testLongGE(String fileName, int column, long a) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongCheck longcheck = new LongCheck(parser, "Date Test",column, NumericCheck.NumericCheckType.GE, a,0L);
        fileChecker.addCheck(longcheck);
        assertTrue("ERROR PASSING GE TEST FOR FILE "+fileName+" column "+column+" greater or equal "+a,fileChecker.run(1));
    }

    public void testLongBetween(String fileName, int column, long a, long b) {
        FileChecker fileChecker = new FileChecker(fileName);
        LongParser parser = new LongParser();
        LongCheck longcheck = new LongCheck(parser, "Date Test",column, NumericCheck.NumericCheckType.BETWEEN, a,b);
        fileChecker.addCheck(longcheck);
        assertTrue("ERROR PASSING BETWEENS TEST FOR FILE "+fileName+" column "+column+" between "+a+" and "+b,fileChecker.run(1));
    }

    public void testPairUniquenessPlusExistance(String relationFileName, int columnA, int columnB, String entityFileNameA, int entityColumnA, String entityFileNameB, int entityColumnB) {
        LongParser parser = new LongParser();
        ColumnSet<Long> entitiesA = new ColumnSet<>(parser,new File(entityFileNameA),entityColumnA,1);
        ColumnSet<Long> entitiesB = new ColumnSet<>(parser,new File(entityFileNameB),entityColumnB,1);
        FileChecker fileChecker = new FileChecker(relationFileName);
        PairUniquenessCheck pairUniquenessCheck = new PairUniquenessCheck<>(parser,parser,columnA,columnB);
        fileChecker.addCheck(pairUniquenessCheck);
        List<ColumnSet<Long>> entityARefColumns = new ArrayList<>();
        entityARefColumns.add(entitiesA);
        List<ColumnSet<Long>> entityBRefColumns = new ArrayList<>();
        entityBRefColumns.add(entitiesB);
        List<Integer> organisationIndices = new ArrayList<>();
        organisationIndices.add(columnA);
        List<Integer> placeIndices = new ArrayList<>();
        placeIndices.add(columnB);
        ExistsCheck<Long> existsEntityACheck = new ExistsCheck<>(parser,organisationIndices, entityARefColumns);
        ExistsCheck<Long> existsEntityBCheck = new ExistsCheck<>(parser,placeIndices, entityBRefColumns);
        fileChecker.addCheck(existsEntityACheck);
        fileChecker.addCheck(existsEntityBCheck);
        assertTrue("ERROR PASSING ORGANISATION_ISLOCATEDIN_PLACE TEST",fileChecker.run(1));

    }

    public void testPairUniquenessPlusExistance(String relationFileName, int columnA, int columnB, String entityFileName, int entityColumn) {
        LongParser parser = new LongParser();
        ColumnSet<Long> entities = new ColumnSet<>(parser,new File(entityFileName),entityColumn,1);
        FileChecker fileChecker = new FileChecker(relationFileName);
        PairUniquenessCheck pairUniquenessCheck = new PairUniquenessCheck<>(parser,parser,columnA,columnB);
        fileChecker.addCheck(pairUniquenessCheck);
        List<ColumnSet<Long>> refcolumns = new ArrayList<>();
        refcolumns.add(entities);
        List<Integer> columnIndices = new ArrayList<>();
        columnIndices.add(columnA);
        columnIndices.add(columnB);
        ExistsCheck existsCheck = new ExistsCheck<>(parser,columnIndices, refcolumns);
        fileChecker.addCheck(existsCheck);
        assertTrue("ERROR PASSING "+relationFileName+" TEST",fileChecker.run(1));
    }

    public void testIdExistance(String fileToCheckExistanceOf, int columnToCheckExistanceOf, String fileToCheckExistanceAgainst, int columnToCheckExistanceAgainst) {
        LongParser parser = new LongParser();
        ColumnSet<Long> checkAgainstEntities = new ColumnSet<>(parser,new File(fileToCheckExistanceAgainst),columnToCheckExistanceAgainst,1);
        FileChecker fileChecker = new FileChecker(fileToCheckExistanceOf);
        List<ColumnSet<Long>> refcolumns = new ArrayList<>();
        refcolumns.add(checkAgainstEntities);
        List<Integer> columnIndices = new ArrayList<>();
        columnIndices.add(columnToCheckExistanceOf);
        ExistsCheck existsCheck = new ExistsCheck<>(parser,columnIndices, refcolumns);
        fileChecker.addCheck(existsCheck);
        assertTrue("ERROR PASSING "+fileToCheckExistanceOf+" ID EXISTANCE TEST",fileChecker.run(1));
    }

    public void testStringLength(String fileToCheckExistanceOf, int columnToCheckExistanceOf, int length) {
        FileChecker fileChecker = new FileChecker(fileToCheckExistanceOf);
        StringLengthCheck lengthCheck = new StringLengthCheck(columnToCheckExistanceOf, length);
        fileChecker.addCheck(lengthCheck);
        assertTrue("ERROR PASSING "+fileToCheckExistanceOf+" ID EXISTANCE TEST",fileChecker.run(1));
    }

}
