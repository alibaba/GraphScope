

package ldbc.snb.datagen;

import ldbc.snb.datagen.generator.distribution.DegreeDistribution;
import org.apache.hadoop.conf.Configuration;

public class DatagenParams {

    // Files and folders
    public static final String DICTIONARY_DIRECTORY = "/dictionaries/";
    public static final String SPARKBENCH_DIRECTORY = "/sparkbench";
    public static final String IPZONE_DIRECTORY = "/ipaddrByCountries";
    public static final String PERSON_COUNTS_FILE = "personFactors.txt";
    public static final String ACTIVITY_FILE = "activityFactors.txt";

    // Dictionaries dataset files
    public static final String browserDictonryFile = DICTIONARY_DIRECTORY + "browsersDic.txt";
    public static final String companiesDictionaryFile = DICTIONARY_DIRECTORY + "companiesByCountry.txt";
    public static final String countryAbbrMappingFile = DICTIONARY_DIRECTORY + "countryAbbrMapping.txt";
    public static final String popularTagByCountryFile = DICTIONARY_DIRECTORY + "popularTagByCountry.txt";
    public static final String countryDictionaryFile = DICTIONARY_DIRECTORY + "dicLocations.txt";
    public static final String tagsFile = DICTIONARY_DIRECTORY + "tags.txt";
    public static final String emailDictionaryFile = DICTIONARY_DIRECTORY + "email.txt";
    public static final String nameDictionaryFile = DICTIONARY_DIRECTORY + "givennameByCountryBirthPlace.txt.freq.full";
    public static final String universityDictionaryFile = DICTIONARY_DIRECTORY + "universities.txt";
    public static final String cityDictionaryFile = DICTIONARY_DIRECTORY + "citiesByCountry.txt";
    public static final String languageDictionaryFile = DICTIONARY_DIRECTORY + "languagesByCountry.txt";
    public static final String popularDictionaryFile = DICTIONARY_DIRECTORY + "popularPlacesByCountry.txt";
    public static final String agentFile = DICTIONARY_DIRECTORY + "smartPhonesProviders.txt";
    public static final String surnamDictionaryFile = DICTIONARY_DIRECTORY + "surnameByCountryBirthPlace.txt.freq.sort";
    public static final String tagClassFile = DICTIONARY_DIRECTORY + "tagClasses.txt";
    public static final String tagClassHierarchyFile = DICTIONARY_DIRECTORY + "tagClassHierarchy.txt";
    public static final String tagTextFile = DICTIONARY_DIRECTORY + "tagText.txt";
    public static final String tagMatrixFile = DICTIONARY_DIRECTORY + "tagMatrix.txt";
    public static final String flashmobDistFile = DICTIONARY_DIRECTORY + "flashmobDist.txt";
    public static final String fbSocialDegreeFile = DICTIONARY_DIRECTORY + "facebookBucket100.dat";

    //private parameters
    private enum ParameterNames {
        BASE_CORRELATED("ldbc.snb.datagen.generator.baseProbCorrelated"),
        BEST_UNIVERSTY_RATIO("ldbc.snb.datagen.generator.probTopUniv"),
        BLOCK_SIZE("ldbc.snb.datagen.generator.blockSize"),
        COMPANY_UNCORRELATED_RATIO("ldbc.snb.datagen.generator.probUnCorrelatedCompany"),
        DIFFERENT_IP_IN_TRAVEL_RATIO("ldbc.snb.datagen.generator.probDiffIPinTravelSeason"),
        DIFFERENT_IP_NOT_TRAVEL_RATIO("ldbc.snb.datagen.generator.probDiffIPnotTravelSeason"),
        ENGLISH_RATIO("ldbc.snb.datagen.generator.probEnglish"),
        FLASHMOB_TAGS_PER_MONTH("ldbc.snb.datagen.generator.flashmobTagsPerMonth"),
        FLASHMOB_TAG_DIST_EXP("ldbc.snb.datagen.generator.flashmobTagDistExp"),
        FLASHMOB_TAG_MAX_LEVEL("ldbc.snb.datagen.generator.flashmobTagMaxLevel"),
        FLASHMOB_TAG_MIN_LEVEL("ldbc.snb.datagen.generator.flashmobTagMinLevel"),
        GROUP_MAX_POST_MONTH("ldbc.snb.datagen.generator.maxNumGroupPostPerMonth"),
        GROUP_MODERATOR_RATIO("ldbc.snb.datagen.generator.groupModeratorProb"),
        LARGE_COMMENT_RATIO("ldbc.snb.datagen.generator.ratioLargeComment"),
        LARGE_POST_RATIO("ldbc.snb.datagen.generator.ratioLargePost"),
        LIMIT_CORRELATED("ldbc.snb.datagen.generator.limitProCorrelated"),
        MAX_COMMENT_POST("ldbc.snb.datagen.generator.maxNumComments"),
        MAX_COMMENT_SIZE("ldbc.snb.datagen.generator.maxCommentSize"),
        MAX_COMPANIES("ldbc.snb.datagen.generator.maxCompanies"),
        MAX_EMAIL("ldbc.snb.datagen.generator.maxEmails"),
        MAX_FRIENDS("ldbc.snb.datagen.generator.maxNumFriends"),
        MAX_GROUP_MEMBERS("ldbc.snb.datagen.generator.maxNumMemberGroup"),
        MAX_LARGE_COMMENT_SIZE("ldbc.snb.datagen.generator.maxLargeCommentSize"),
        MAX_LARGE_POST_SIZE("ldbc.snb.datagen.generator.maxLargePostSize"),
        MAX_NUM_FLASHMOB_POST_PER_MONTH("ldbc.snb.datagen.generator.maxNumFlashmobPostPerMonth"),
        MAX_NUM_GROUP_FLASHMOB_POST_PER_MONTH("ldbc.snb.datagen.generator.maxNumGroupFlashmobPostPerMonth"),
        MAX_NUM_TAG_PER_FLASHMOB_POST("ldbc.snb.datagen.generator.maxNumTagPerFlashmobPost"),
        MAX_PHOTOALBUM("ldbc.snb.datagen.generator.maxNumPhotoAlbumsPerMonth"),
        MAX_PHOTO_PER_ALBUM("ldbc.snb.datagen.generator.maxNumPhotoPerAlbums"),
        MAX_POPULAR_PLACES("ldbc.snb.datagen.generator.maxNumPopularPlaces"),
        MAX_TEXT_SIZE("ldbc.snb.datagen.generator.maxTextSize"),
        MIN_COMMENT_SIZE("ldbc.snb.datagen.generator.minCommentSize"),
        MIN_LARGE_COMMENT_SIZE("ldbc.snb.datagen.generator.minLargeCommentSize"),
        MIN_LARGE_POST_SIZE("ldbc.snb.datagen.generator.minLargePostSize"),
        MIN_TEXT_SIZE("ldbc.snb.datagen.generator.minTextSize"),
        MISSING_RATIO("ldbc.snb.datagen.generator.missingRatio"),
        OTHER_BROWSER_RATIO("ldbc.snb.datagen.generator.probAnotherBrowser"),
        POPULAR_PLACE_RATIO("ldbc.snb.datagen.generator.probPopularPlaces"),
        PROB_INTEREST_FLASHMOB_TAG("ldbc.snb.datagen.generator.probInterestFlashmobTag"),
        PROB_RANDOM_PER_LEVEL("ldbc.snb.datagen.generator.probRandomPerLevel"),
        REDUCE_TEXT_RATIO("ldbc.snb.datagen.generator.ratioReduceText"),
        SECOND_LANGUAGE_RATIO("ldbc.snb.datagen.generator.probSecondLang"),
        TAG_UNCORRELATED_COUNTRY("ldbc.snb.datagen.generator.tagCountryCorrProb"),
        UNIVERSITY_UNCORRELATED_RATIO("ldbc.snb.datagen.generator.probUnCorrelatedOrganization"),
        MAX_NUM_LIKE("ldbc.snb.datagen.generator.maxNumLike"),
        UPDATE_PORTION("ldbc.snb.datagen.serializer.updatePortion"),
        USER_MAX_GROUP("ldbc.snb.datagen.generator.maxNumGroupCreatedPerUser"),
        USER_MAX_POST_MONTH("ldbc.snb.datagen.generator.maxNumPostPerMonth"),
        USER_MAX_TAGS("ldbc.snb.datagen.generator.maxNumTagsPerUser"),
        USER_MIN_TAGS("ldbc.snb.datagen.generator.minNumTagsPerUser");

        private final String name;

        ParameterNames(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    public static double baseProbCorrelated = 0.0; // the base probability to create a correlated edge between two persons
    public static double flashmobTagDistExp = 0.0; // the flashmob tag distribution exponent
    public static double flashmobTagMaxLevel = 0.0; // the flashmob tag max activity volume level
    public static double flashmobTagMinLevel = 0.0; // the flashmob tag min activity volume level
    public static double groupModeratorProb = 0.0;
    public static double limitProCorrelated = 0.0;
    public static double missingRatio = 0.0;
    public static double probAnotherBrowser = 0.0;
    public static double probDiffIPinTravelSeason = 0.0; // in travel season
    public static double probDiffIPnotTravelSeason = 0.0; // not in travel season
    public static double probEnglish = 0.0;
    public static double probInterestFlashmobTag = 0.0;
    public static double probPopularPlaces = 0.0; //probability of taking a photo at popular place
    public static double probRandomPerLevel = 0.0;
    public static double probSecondLang = 0.0;
    public static double probTopUniv = 0.0; // 90% users go to top university
    public static double probUnCorrelatedCompany = 0.0;
    public static double probUnCorrelatedOrganization = 0.0;
    public static double ratioLargeComment = 0.0;
    public static double ratioLargePost = 0.0;
    public static double ratioReduceText = 0.0; // 80% text has size less than 1/2 max size
    public static double tagCountryCorrProb = 0.0;
    public static double updatePortion = 0.0;
    public static int blockSize = 0;
    public static int flashmobTagsPerMonth = 0;
    public static int maxCommentSize = 0;
    public static int maxCompanies = 0;
    public static int maxEmails = 0;
    public static int maxLargeCommentSize = 0;
    public static int maxLargePostSize = 0;
    public static int maxNumComments = 0;
    public static int maxNumFlashmobPostPerMonth = 0;
    public static int maxNumFriends = 0;
    public static int maxNumGroupCreatedPerUser = 0;
    public static int maxNumGroupFlashmobPostPerMonth = 0;
    public static int maxNumGroupPostPerMonth = 0;
    public static int maxNumMemberGroup = 0;
    public static int maxNumLike = 0;
    public static int maxNumPhotoAlbumsPerMonth = 0;
    public static int maxNumPhotoPerAlbums = 0;
    public static int maxNumPopularPlaces = 0;
    public static int maxNumPostPerMonth = 0;
    public static int maxNumTagPerFlashmobPost = 0;
    public static int maxNumTagsPerUser = 0;
    public static int maxTextSize = 0;
    public static int minCommentSize = 0;
    public static int minLargeCommentSize = 0;
    public static int minLargePostSize = 0;
    public static int minNumTagsPerUser = 0;
    public static int minTextSize = 0;


    public static final int startMonth = 0;
    public static final int startDate = 1;
    public static final int endMonth = 0;
    public static final int endDate = 1;
    public static final double alpha = 0.4;


    public static String outputDir = "./";
    public static String hadoopDir = "./";
    public static String socialNetworkDir = "./";
    public static int numThreads = 1;
    public static int deltaTime = 10000;
    public static long numPersons = 10000;
    public static int startYear = 2010;
    public static int endYear = 2013;
    public static int numYears = 3;
    public static boolean updateStreams = false;
    public static boolean exportText = true;
    public static boolean compressed = false;
    public static int numPartitions = 1;
    public static int numUpdatePartitions = 1;


    public static void readConf(Configuration conf) {
        try {

            ParameterNames values[] = ParameterNames.values();
            for (int i = 0; i < values.length; ++i) {
                if (conf.get(values[i].toString()) == null) {
                    throw new IllegalStateException("Missing " + values[i].toString() + " parameter");
                }
            }

            maxNumFriends = Integer.parseInt(conf.get(ParameterNames.MAX_FRIENDS.toString()));
            minNumTagsPerUser = Integer.parseInt(conf.get(ParameterNames.USER_MIN_TAGS.toString()));
            maxNumTagsPerUser = Integer.parseInt(conf.get(ParameterNames.USER_MAX_TAGS.toString()));
            maxNumPostPerMonth = Integer.parseInt(conf.get(ParameterNames.USER_MAX_POST_MONTH.toString()));
            maxNumComments = Integer.parseInt(conf.get(ParameterNames.MAX_COMMENT_POST.toString()));
            limitProCorrelated = Double.parseDouble(conf.get(ParameterNames.LIMIT_CORRELATED.toString()));
            baseProbCorrelated = Double.parseDouble(conf.get(ParameterNames.BASE_CORRELATED.toString()));
            maxEmails = Integer.parseInt(conf.get(ParameterNames.MAX_EMAIL.toString()));
            maxCompanies = Integer.parseInt(conf.get(ParameterNames.MAX_EMAIL.toString()));
            probEnglish = Double.parseDouble(conf.get(ParameterNames.MAX_EMAIL.toString()));
            probSecondLang = Double.parseDouble(conf.get(ParameterNames.MAX_EMAIL.toString()));
            probAnotherBrowser = Double.parseDouble(conf.get(ParameterNames.OTHER_BROWSER_RATIO.toString()));
            minTextSize = Integer.parseInt(conf.get(ParameterNames.MIN_TEXT_SIZE.toString()));
            maxTextSize = Integer.parseInt(conf.get(ParameterNames.MAX_TEXT_SIZE.toString()));
            minCommentSize = Integer.parseInt(conf.get(ParameterNames.MIN_COMMENT_SIZE.toString()));
            maxCommentSize = Integer.parseInt(conf.get(ParameterNames.MAX_COMMENT_SIZE.toString()));
            ratioReduceText = Double.parseDouble(conf.get(ParameterNames.REDUCE_TEXT_RATIO.toString()));
            minLargePostSize = Integer.parseInt(conf.get(ParameterNames.MIN_LARGE_POST_SIZE.toString()));
            maxLargePostSize = Integer.parseInt(conf.get(ParameterNames.MAX_LARGE_POST_SIZE.toString()));
            minLargeCommentSize = Integer.parseInt(conf.get(ParameterNames.MIN_LARGE_COMMENT_SIZE.toString()));
            maxLargeCommentSize = Integer.parseInt(conf.get(ParameterNames.MAX_LARGE_COMMENT_SIZE.toString()));
            ratioLargePost = Double.parseDouble(conf.get(ParameterNames.LARGE_POST_RATIO.toString()));
            ratioLargeComment = Double.parseDouble(conf.get(ParameterNames.LARGE_COMMENT_RATIO.toString()));
            maxNumLike = Integer.parseInt(conf.get(ParameterNames.MAX_NUM_LIKE.toString()));
            maxNumPhotoAlbumsPerMonth = Integer.parseInt(conf.get(ParameterNames.MAX_PHOTOALBUM.toString()));
            maxNumPhotoPerAlbums = Integer.parseInt(conf.get(ParameterNames.MAX_PHOTO_PER_ALBUM.toString()));
            maxNumGroupCreatedPerUser = Integer.parseInt(conf.get(ParameterNames.USER_MAX_GROUP.toString()));
            maxNumMemberGroup = Integer.parseInt(conf.get(ParameterNames.MAX_GROUP_MEMBERS.toString()));
            groupModeratorProb = Double.parseDouble(conf.get(ParameterNames.GROUP_MODERATOR_RATIO.toString()));
            maxNumGroupPostPerMonth = Integer.parseInt(conf.get(ParameterNames.GROUP_MAX_POST_MONTH.toString()));
            missingRatio = Double.parseDouble(conf.get(ParameterNames.MISSING_RATIO.toString()));
            probDiffIPinTravelSeason = Double.parseDouble(conf.get(ParameterNames.DIFFERENT_IP_IN_TRAVEL_RATIO
                                                                           .toString()));
            probDiffIPnotTravelSeason = Double.parseDouble(conf.get(ParameterNames.DIFFERENT_IP_NOT_TRAVEL_RATIO
                                                                            .toString()));
            probUnCorrelatedCompany = Double.parseDouble(conf.get(ParameterNames.COMPANY_UNCORRELATED_RATIO
                                                                          .toString()));
            probUnCorrelatedOrganization = Double.parseDouble(conf.get(ParameterNames.UNIVERSITY_UNCORRELATED_RATIO
                                                                               .toString()));
            probTopUniv = Double.parseDouble(conf.get(ParameterNames.BEST_UNIVERSTY_RATIO.toString()));
            maxNumPopularPlaces = Integer.parseInt(conf.get(ParameterNames.MAX_POPULAR_PLACES.toString()));
            probPopularPlaces = Double.parseDouble(conf.get(ParameterNames.POPULAR_PLACE_RATIO.toString()));
            tagCountryCorrProb = Double.parseDouble(conf.get(ParameterNames.TAG_UNCORRELATED_COUNTRY.toString()));
            flashmobTagsPerMonth = Integer.parseInt(conf.get(ParameterNames.FLASHMOB_TAGS_PER_MONTH.toString()));
            probInterestFlashmobTag = Double.parseDouble(conf.get(ParameterNames.PROB_INTEREST_FLASHMOB_TAG
                                                                          .toString()));
            probRandomPerLevel = Double.parseDouble(conf.get(ParameterNames.PROB_RANDOM_PER_LEVEL.toString()));
            maxNumFlashmobPostPerMonth = Integer.parseInt(conf.get(ParameterNames.MAX_NUM_FLASHMOB_POST_PER_MONTH
                                                                           .toString()));
            maxNumGroupFlashmobPostPerMonth = Integer
                    .parseInt(conf.get(ParameterNames.MAX_NUM_GROUP_FLASHMOB_POST_PER_MONTH.toString()));
            maxNumTagPerFlashmobPost = Integer.parseInt(conf.get(ParameterNames.MAX_NUM_TAG_PER_FLASHMOB_POST
                                                                         .toString()));
            flashmobTagMinLevel = Double.parseDouble(conf.get(ParameterNames.FLASHMOB_TAG_MIN_LEVEL.toString()));
            flashmobTagMaxLevel = Double.parseDouble(conf.get(ParameterNames.FLASHMOB_TAG_MAX_LEVEL.toString()));
            flashmobTagDistExp = Double.parseDouble(conf.get(ParameterNames.FLASHMOB_TAG_DIST_EXP.toString()));
            updatePortion = Double.parseDouble(conf.get(ParameterNames.UPDATE_PORTION.toString()));
            blockSize = Integer.parseInt(conf.get(ParameterNames.BLOCK_SIZE.toString()));

        } catch (Exception e) {
            System.out.println("Error reading scale factors");
            System.err.println(e.getMessage());
            throw e;
        }

        try {
            numPersons = Long.parseLong(conf.get("ldbc.snb.datagen.generator.numPersons"));
            startYear = Integer.parseInt(conf.get("ldbc.snb.datagen.generator.startYear"));
            numYears = Integer.parseInt(conf.get("ldbc.snb.datagen.generator.numYears"));
            endYear = startYear + numYears;
            compressed = conf.getBoolean("ldbc.snb.datagen.serializer.compressed", false);
            numThreads = conf.getInt("ldbc.snb.datagen.generator.numThreads", 1);
            updateStreams = conf.getBoolean("ldbc.snb.datagen.serializer.updateStreams", false);
            numPartitions = conf.getInt("ldbc.snb.datagen.serializer.numPartitions", 1);
            numUpdatePartitions = conf.getInt("ldbc.snb.datagen.serializer.numUpdatePartitions", 1);
            deltaTime = conf.getInt("ldbc.snb.datagen.generator.deltaTime", 10000);
            outputDir = conf.get("ldbc.snb.datagen.serializer.outputDir");
            hadoopDir = outputDir + "/hadoop";
            socialNetworkDir = outputDir + "social_network";
            if (conf.get("ldbc.snb.datagen.generator.gscale") != null) {
                double scale = conf.getDouble("ldbc.snb.datagen.generator.gscale", 6.0);
                String degreeDistributionName = conf.get("ldbc.snb.datagen.generator.distribution.degreeDistribution");
                DegreeDistribution degreeDistribution = (DegreeDistribution) Class.forName(degreeDistributionName)
                                                                                  .newInstance();
                degreeDistribution.initialize(conf);
                numPersons = findNumPersonsFromGraphalyticsScale(degreeDistribution, scale);
            }
            System.out.println(" ... Num Persons " + numPersons);
            System.out.println(" ... Start Year " + startYear);
            System.out.println(" ... Num Years " + numYears);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static double scale(long numPersons, double mean) {
        return Math.log10(mean * numPersons / 2 + numPersons);
    }

    private static long findNumPersonsFromGraphalyticsScale(DegreeDistribution distribution, double scale) {

        long numPersonsMin = 1000000;
        while (scale(numPersonsMin, distribution.mean(numPersonsMin)) > scale) {
            numPersonsMin /= 2;
        }

        long numPersonsMax = 1000000;
        while (scale(numPersonsMax, distribution.mean(numPersonsMax)) < scale) {
            numPersonsMax *= 2;
        }

        long currentNumPersons = (numPersonsMax - numPersonsMin) / 2 + numPersonsMin;
        double currentScale = scale(currentNumPersons, distribution.mean(currentNumPersons));
        while (Math.abs(currentScale - scale) / scale > 0.001) {
            if (currentScale < scale) {
                numPersonsMin = currentNumPersons;
            } else {
                numPersonsMax = currentNumPersons;
            }
            currentNumPersons = (numPersonsMax - numPersonsMin) / 2 + numPersonsMin;
            currentScale = scale(currentNumPersons, distribution.mean(currentNumPersons));
            System.out.println(numPersonsMin + " " + numPersonsMax + " " + currentNumPersons + " " + currentScale);
        }
        return currentNumPersons;
    }
}
