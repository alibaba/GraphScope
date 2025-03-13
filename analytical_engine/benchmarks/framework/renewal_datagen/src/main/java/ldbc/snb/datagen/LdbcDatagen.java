package ldbc.snb.datagen;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.hadoop.generator.HadoopKnowsGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonActivityGenerator;
import ldbc.snb.datagen.hadoop.generator.HadoopPersonGenerator;
import ldbc.snb.datagen.hadoop.miscjob.HadoopMergeFriendshipFiles;
import ldbc.snb.datagen.hadoop.serializer.HadoopPersonSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopPersonSortAndSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopStaticSerializer;
import ldbc.snb.datagen.hadoop.serializer.HadoopUpdateStreamSorterAndSerializer;
import ldbc.snb.datagen.util.ConfigParser;
import ldbc.snb.datagen.vocabulary.SN;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class LdbcDatagen {
    private static boolean initialized = false;

    public static synchronized void initializeContext(Configuration conf) {
        if (!initialized) {
            DatagenParams.readConf(conf);
            Dictionaries.loadDictionaries(conf);
            SN.initialize();
            try {
                Person.personSimilarity = (Person.PersonSimilarity) Class
                        .forName(conf.get("ldbc.snb.datagen.generator.person.similarity")).newInstance();
            } catch (Exception e) {
                System.err.println("Error while loading person similarity class");
                System.err.println(e.getMessage());
            }
            initialized = true;
        }
    }

    private void printProgress(String message) {
        System.out.println("************************************************");
        System.out.println("* " + message + " *");
        System.out.println("************************************************");
    }

    public int runGenerateJob(Configuration conf) throws Exception {

        String hadoopPrefix = conf.get("ldbc.snb.datagen.serializer.hadoopDir");
        FileSystem fs = FileSystem.get(conf);
        List<Float> percentages = new ArrayList<>();
        percentages.add(0.45f);
        percentages.add(0.45f);
        percentages.add(0.1f);

        long start = System.currentTimeMillis();
        printProgress("Starting: Person generation");
        long startPerson = System.currentTimeMillis();
        HadoopPersonGenerator personGenerator = new HadoopPersonGenerator(conf);
        personGenerator.run(hadoopPrefix + "/persons", "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter");
        long endPerson = System.currentTimeMillis();

        printProgress("Creating university location correlated edges");
        long startUniversity = System.currentTimeMillis();
        HadoopKnowsGenerator knowsGenerator = new HadoopKnowsGenerator(conf,
                                                                       "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter",
                                                                       "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                                                                       percentages,
                                                                       0,
                                                                       conf.get("ldbc.snb.datagen.generator.knowsGenerator"));

        knowsGenerator.run(hadoopPrefix + "/persons", hadoopPrefix + "/universityEdges");
        long endUniversity = System.currentTimeMillis();


        printProgress("Creating main interest correlated edges");
        long startInterest = System.currentTimeMillis();

        knowsGenerator = new HadoopKnowsGenerator(conf,
                                                  "ldbc.snb.datagen.hadoop.miscjob.keychanger.InterestKeySetter",
                                                  "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                                                  percentages,
                                                  1,
                                                  conf.get("ldbc.snb.datagen.generator.knowsGenerator"));

        knowsGenerator.run(hadoopPrefix + "/persons", hadoopPrefix + "/interestEdges");
        long endInterest = System.currentTimeMillis();


        printProgress("Creating random correlated edges");
        long startRandom = System.currentTimeMillis();

        knowsGenerator = new HadoopKnowsGenerator(conf,
                                                  "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                                                  "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
                                                  percentages,
                                                  2,
                                                  "ldbc.snb.datagen.generator.generators.knowsgenerators.RandomKnowsGenerator");

        knowsGenerator.run(hadoopPrefix + "/persons", hadoopPrefix + "/randomEdges");
        long endRandom = System.currentTimeMillis();


        fs.delete(new Path(DatagenParams.hadoopDir + "/persons"), true);
        printProgress("Merging the different edge files");
        List<String> edgeFileNames = new ArrayList<>();
        edgeFileNames.add(hadoopPrefix + "/universityEdges");
        edgeFileNames.add(hadoopPrefix + "/interestEdges");
        edgeFileNames.add(hadoopPrefix + "/randomEdges");
        long startMerge = System.currentTimeMillis();
        HadoopMergeFriendshipFiles merger = new HadoopMergeFriendshipFiles(conf, "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter");
        merger.run(hadoopPrefix + "/mergedPersons", edgeFileNames);
        long endMerge = System.currentTimeMillis();

        printProgress("Serializing persons");
        long startPersonSerializing = System.currentTimeMillis();
        if (conf.getBoolean("ldbc.snb.datagen.serializer.persons.sort", true)) {
            HadoopPersonSortAndSerializer serializer = new HadoopPersonSortAndSerializer(conf);
            serializer.run(hadoopPrefix + "/mergedPersons");
        } else {
            HadoopPersonSerializer serializer = new HadoopPersonSerializer(conf);
            serializer.run(hadoopPrefix + "/mergedPersons");
        }
        long endPersonSerializing = System.currentTimeMillis();

        long startPersonActivity = System.currentTimeMillis();
        if (conf.getBoolean("ldbc.snb.datagen.generator.activity", true)) {
            printProgress("Generating and serializing person activity");
            HadoopPersonActivityGenerator activityGenerator = new HadoopPersonActivityGenerator(conf);
            activityGenerator.run(hadoopPrefix + "/mergedPersons");

            int numThreads = DatagenParams.numThreads;
            int blockSize = DatagenParams.blockSize;
            int numBlocks = (int) Math.ceil(DatagenParams.numPersons / (double) blockSize);

            for (int i = 0; i < numThreads; ++i) {
                if (i < numBlocks) {
                    fs.copyToLocalFile(false, new Path(DatagenParams.hadoopDir + "/m" + i + "personFactors.txt"), new Path("./"));
                    fs.copyToLocalFile(false, new Path(DatagenParams.hadoopDir + "/m" + i + "activityFactors.txt"), new Path("./"));
                    fs.copyToLocalFile(false, new Path(DatagenParams.hadoopDir + "/m0friendList" + i + ".csv"), new Path("./"));
                }
            }
        }
        long endPersonActivity = System.currentTimeMillis();

        long startSortingUpdateStreams = System.currentTimeMillis();

        if (conf.getBoolean("ldbc.snb.datagen.serializer.updateStreams", false)) {

            printProgress("Sorting update streams ");

            List<String> personStreamsFileNames = new ArrayList<>();
            List<String> forumStreamsFileNames = new ArrayList<>();
            for (int i = 0; i < DatagenParams.numThreads; ++i) {
                int numPartitions = conf.getInt("ldbc.snb.datagen.serializer.numUpdatePartitions", 1);
                for (int j = 0; j < numPartitions; ++j) {
                    personStreamsFileNames.add(DatagenParams.hadoopDir + "/temp_updateStream_person_" + i + "_" + j);
                    if (conf.getBoolean("ldbc.snb.datagen.generator.activity", false)) {
                        forumStreamsFileNames.add(DatagenParams.hadoopDir + "/temp_updateStream_forum_" + i + "_" + j);
                    }
                }
            }
            HadoopUpdateStreamSorterAndSerializer updateSorterAndSerializer = new HadoopUpdateStreamSorterAndSerializer(conf);
            updateSorterAndSerializer.run(personStreamsFileNames, "person");
            updateSorterAndSerializer.run(forumStreamsFileNames, "forum");
            for (String file : personStreamsFileNames) {
                fs.delete(new Path(file), true);
            }

            for (String file : forumStreamsFileNames) {
                fs.delete(new Path(file), true);
            }

            long minDate = Long.MAX_VALUE;
            long maxDate = Long.MIN_VALUE;
            long count = 0;
            for (int i = 0; i < DatagenParams.numThreads; ++i) {
                Path propertiesFile = new Path(DatagenParams.hadoopDir + "/temp_updateStream_person_" + i + ".properties");
                FSDataInputStream file = fs.open(propertiesFile);
                Properties properties = new Properties();
                properties.load(file);
                long aux;
                aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.min_write_event_start_time"));
                minDate = aux < minDate ? aux : minDate;
                aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.max_write_event_start_time"));
                maxDate = aux > maxDate ? aux : maxDate;
                aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.num_events"));
                count += aux;
                file.close();
                fs.delete(propertiesFile, true);

                if (conf.getBoolean("ldbc.snb.datagen.generator.activity", false)) {
                    propertiesFile = new Path(DatagenParams.hadoopDir + "/temp_updateStream_forum_" + i + ".properties");
                    file = fs.open(propertiesFile);
                    properties = new Properties();
                    properties.load(file);
                    aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.min_write_event_start_time"));
                    minDate = aux < minDate ? aux : minDate;
                    aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.max_write_event_start_time"));
                    maxDate = aux > maxDate ? aux : maxDate;
                    aux = Long.parseLong(properties.getProperty("ldbc.snb.interactive.num_events"));
                    count += aux;
                    file.close();
                    fs.delete(propertiesFile, true);
                }
            }

            OutputStream output = fs
                    .create(new Path(DatagenParams.socialNetworkDir + "/updateStream" + ".properties"), true);
            output.write(new String("ldbc.snb.interactive.gct_delta_duration:" + DatagenParams.deltaTime + "\n")
                                 .getBytes());
            output.write(new String("ldbc.snb.interactive.min_write_event_start_time:" + minDate + "\n").getBytes());
            output.write(new String("ldbc.snb.interactive.max_write_event_start_time:" + maxDate + "\n").getBytes());
            output.write(new String("ldbc.snb.interactive.update_interleave:" + (maxDate - minDate) / count + "\n")
                                 .getBytes());
            output.write(new String("ldbc.snb.interactive.num_events:" + count).getBytes());
            output.close();
        }

        long endSortingUpdateStreams = System.currentTimeMillis();

        printProgress("Serializing static graph ");
        long startInvariantSerializing = System.currentTimeMillis();
        HadoopStaticSerializer staticSerializer = new HadoopStaticSerializer(conf);
        staticSerializer.run();
        long endInvariantSerializing = System.currentTimeMillis();

        long end = System.currentTimeMillis();

        System.out.println(((end - start) / 1000)
                                   + " total seconds");
        System.out.println("Person generation time: " + ((endPerson - startPerson) / 1000));
        System.out.println("University correlated edge generation time: " + ((endUniversity - startUniversity) / 1000));
        System.out.println("Interest correlated edge generation time: " + ((endInterest - startInterest) / 1000));
        System.out.println("Random correlated edge generation time: " + ((endRandom - startRandom) / 1000));
        System.out.println("Edges merge time: " + ((endMerge - startMerge) / 1000));
        System.out.println("Person serialization time: " + ((endPersonSerializing - startPersonSerializing) / 1000));
        System.out
                .println("Person activity generation and serialization time: " + ((endPersonActivity - startPersonActivity) / 1000));
        System.out
                .println("Sorting update streams time: " + ((endSortingUpdateStreams - startSortingUpdateStreams) / 1000));
        System.out
                .println("Invariant schema serialization time: " + ((endInvariantSerializing - startInvariantSerializing) / 1000));
        System.out.println("Total Execution time: " + ((end - start) / 1000));

        if (conf.getBoolean("ldbc.snb.datagen.parametergenerator.parameters", false) && conf
                .getBoolean("ldbc.snb.datagen.generator.activity", false)) {
            System.out.println("Running Parameter Generation");
            System.out.println("Generating Interactive Parameters");
            ProcessBuilder pb1 = new ProcessBuilder("mkdir", "-p", conf
                    .get("ldbc.snb.datagen.serializer.outputDir") + "/substitution_parameters");
            pb1.directory(new File("./"));
            Process p1 = pb1.start();
            int r1 = p1.waitFor();
            if (r1 != 0) {
                throw new RuntimeException("Creating parameter directory failed with exit code " + r1);
            }

            ProcessBuilder pb2 = new ProcessBuilder(conf.get("ldbc.snb.datagen.parametergenerator.python"), "paramgenerator/generateparams.py", "./", conf
                    .get("ldbc.snb.datagen.serializer.outputDir") + "/substitution_parameters");
            pb2.directory(new File("./"));
            File logInteractive = new File("parameters_interactive.log");
            pb2.redirectErrorStream(true);
            pb2.redirectOutput(ProcessBuilder.Redirect.appendTo(logInteractive));
            Process p2 = pb2.start();
            int r2 = p2.waitFor();
            if (r2 != 0) {
                throw new RuntimeException("Parameter generation failed with exit code " + r2);
            }

            System.out.println("Finished Parameter Generation");
        }
        return 0;
    }

    public static void prepareConfiguration(Configuration conf) throws Exception {

        conf.set("ldbc.snb.datagen.serializer.hadoopDir", conf
                .get("ldbc.snb.datagen.serializer.outputDir") + "/hadoop");
        conf.set("ldbc.snb.datagen.serializer.socialNetworkDir", conf
                .get("ldbc.snb.datagen.serializer.outputDir") + "/social_network");

        // Deleting existing files
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir")), true);
        dfs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir")), true);
        FileUtils.deleteDirectory(new File(conf.get("ldbc.snb.datagen.serializer.outputDir")
                                                   + "/substitution_parameters"));

        ConfigParser.printConfig(conf);

    }

    public static void main(String[] args) throws Exception {

        try {
            Configuration conf = ConfigParser.initialize();
            ConfigParser.readConfig(conf, args[0]);
            ConfigParser.readConfig(conf, LdbcDatagen.class.getResourceAsStream("/params_default.ini"));

            LdbcDatagen.prepareConfiguration(conf);
            LdbcDatagen.initializeContext(conf);
            LdbcDatagen datagen = new LdbcDatagen();
            datagen.runGenerateJob(conf);
        } catch (Exception e) {
            System.err.println("Error during execution");
            System.err.println(e.getMessage());
            throw e;
        }
    }
}
