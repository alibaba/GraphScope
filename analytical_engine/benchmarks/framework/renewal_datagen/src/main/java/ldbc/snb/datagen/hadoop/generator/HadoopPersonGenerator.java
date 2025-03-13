package ldbc.snb.datagen.hadoop.generator;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.LdbcDatagen;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.generator.generators.PersonGenerator;
import ldbc.snb.datagen.hadoop.key.TupleKey;
import ldbc.snb.datagen.hadoop.miscjob.keychanger.HadoopFileKeyChanger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;

public class HadoopPersonGenerator {

    private Configuration conf = null;

    public static class HadoopPersonGeneratorMapper extends Mapper<LongWritable, Text, TupleKey, Person> {

        private HadoopFileKeyChanger.KeySetter<TupleKey> keySetter = null;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            try {
                this.keySetter = (HadoopFileKeyChanger.KeySetter) Class.forName(conf.get("postKeySetterName"))
                                                                       .newInstance();
            } catch (Exception e) {
                System.err.println("Error when setting key setter");
                System.err.println(e.getMessage());
                throw new RuntimeException(e);
            }

            int threadId = Integer.parseInt(value.toString());
            System.out.println("Generating user at mapper " + threadId);
            LdbcDatagen.initializeContext(conf);

            // Here we determine the blocks in the "block space" that this mapper is responsible for.
            int numBlocks = (int) (Math.ceil(DatagenParams.numPersons / (double) DatagenParams.blockSize));
            int initBlock = (int) (Math.ceil((numBlocks / (double) DatagenParams.numThreads) * threadId));
            int endBlock = (int) (Math.ceil((numBlocks / (double) DatagenParams.numThreads) * (threadId + 1)));

            PersonGenerator personGenerator = new PersonGenerator(conf, conf
                    .get("ldbc.snb.datagen.generator.distribution.degreeDistribution"));
            for (int i = initBlock; i < endBlock; ++i) {
                Person[] block = personGenerator.generateUserBlock(i, DatagenParams.blockSize);
                int size = block.length;
                for (int j = 0; j < size && DatagenParams.blockSize * i + j < DatagenParams.numPersons; ++j) {
                    try {
                        context.write(keySetter.getKey(block[j]), block[j]);
                    } catch (IOException ioE) {
                        System.err.println("Input/Output Exception when writing to context.");
                        System.err.println(ioE.getMessage());
                        ioE.printStackTrace();
                    } catch (InterruptedException iE) {
                        System.err.println("Interrupted Exception when writing to context.");
                        iE.printStackTrace();
                    }
                }
            }
        }
    }


    public static class HadoopPersonGeneratorReducer extends Reducer<TupleKey, Person, TupleKey, Person> {

        @Override
        public void reduce(TupleKey key, Iterable<Person> valueSet,
                           Context context) throws IOException, InterruptedException {
            for (Person person : valueSet) {
                context.write(key, person);
            }
        }
    }


    public HadoopPersonGenerator(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    private static void writeToOutputFile(String filename, int numMaps, Configuration conf) {
        try {
            FileSystem dfs = FileSystem.get(conf);
            OutputStream output = dfs.create(new Path(filename));
            for (int i = 0; i < numMaps; i++)
                output.write((new String(i + "\n").getBytes()));
            output.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generates a Person hadoop sequence file containing key-value paiers
     * where the key is the person id and the value is the person itself.
     *
     * @param outputFileName The name of the file to store the persons.
     * @throws Exception
     */
    public void run(String outputFileName, String postKeySetterName) throws Exception {

        String hadoopDir = conf.get("ldbc.snb.datagen.serializer.hadoopDir");
        String tempFile = hadoopDir + "/mrInputFile";

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(tempFile), true);
        writeToOutputFile(tempFile, Integer.parseInt(conf.get("ldbc.snb.datagen.generator.numThreads")), conf);

        int numThreads = Integer.parseInt(conf.get("ldbc.snb.datagen.generator.numThreads"));
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 1);
        conf.set("postKeySetterName", postKeySetterName);
        Job job = Job.getInstance(conf, "SIB Generate Users & 1st Dimension");
        job.setMapOutputKeyClass(TupleKey.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(TupleKey.class);
        job.setOutputValueClass(Person.class);
        job.setJarByClass(HadoopPersonGeneratorMapper.class);
        job.setMapperClass(HadoopPersonGeneratorMapper.class);
        job.setReducerClass(HadoopPersonGeneratorReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(tempFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));
        if (!job.waitForCompletion(true)) {
            throw new Exception();
        }
    }
}
