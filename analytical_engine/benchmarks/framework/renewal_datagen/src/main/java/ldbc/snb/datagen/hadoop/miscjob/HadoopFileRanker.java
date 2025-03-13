package ldbc.snb.datagen.hadoop.miscjob;

import ldbc.snb.datagen.LdbcDatagen;
import ldbc.snb.datagen.hadoop.key.TupleKey;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKey;
import ldbc.snb.datagen.hadoop.key.blockkey.BlockKeyComparator;
import ldbc.snb.datagen.hadoop.miscjob.keychanger.HadoopFileKeyChanger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HadoopFileRanker {
    private Configuration conf;
    private Class<?> K;
    private Class<?> V;
    private String keySetterName;

    /**
     * @param conf The configuration object.
     * @param K    The Key class of the hadoop sequence file.
     * @param V    The Value class of the hadoop sequence file.
     */
    public HadoopFileRanker(Configuration conf, Class<?> K, Class<?> V, String keySetter) {
        this.conf = new Configuration(conf);
        this.keySetterName = keySetter;
        this.K = K;
        this.V = V;
    }

    public static class HadoopFileRankerSortMapper<K, V> extends Mapper<K, V, K, V> {

        private HadoopFileKeyChanger.KeySetter<TupleKey> keySetter;

        @Override
        public void setup(Context context) {

            try {
                LdbcDatagen.initializeContext(context.getConfiguration());
                String className = context.getConfiguration().get("keySetterClassName");
                keySetter = (HadoopFileKeyChanger.KeySetter) Class.forName(className).newInstance();
            } catch (ClassNotFoundException e) {
                System.out.print(e.getMessage());
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                System.out.print(e.getMessage());
                throw new RuntimeException(e);
            } catch (InstantiationException e) {
                System.out.print(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void map(K key, V value,
                        Context context) throws IOException, InterruptedException {
            context.write((K) keySetter.getKey(value), value);
        }
    }

    public static class HadoopFileRankerSortReducer<K, V, T extends BlockKey> extends Reducer<K, V, BlockKey, V> {

        private int reducerId;
        /**
         * The id of the reducer.
         **/
        private long counter = 0;

        /**
         * Counter of the number of elements received by this reducer.
         */

        @Override
        public void setup(Context context) {
            reducerId = context.getTaskAttemptID().getTaskID().getId();
        }

        @Override
        public void reduce(K key, Iterable<V> valueSet,
                           Context context) throws IOException, InterruptedException {

            for (V v : valueSet) {
                context.write(new BlockKey(reducerId, new TupleKey(counter++, 0)), v);
            }
        }

        @Override
        public void cleanup(Context context) {
            Configuration conf = context.getConfiguration();
            try {
                FileSystem fs = FileSystem.get(conf);
                DataOutputStream output = fs
                        .create(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/rank_" + reducerId));
                output.writeLong(counter);
                output.close();
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public static class HadoopFileRankerPartitioner<V> extends Partitioner<BlockKey, V> {

        @Override
        public int getPartition(BlockKey key, V value,
                                int numReduceTasks) {
            return (int) (key.block % numReduceTasks);
        }
    }

    public static class HadoopFileRankerFinalReducer<BlockKey, V, T extends LongWritable> extends Reducer<BlockKey, V, LongWritable, V> {

        private int reducerId;
        /**
         * The id of the reducer.
         **/
        private int numReduceTasks;
        /**
         * The number of reducer tasks.
         **/
        private long counters[];
        /**
         * The number of elements processed by each reducer in the previous step.
         **/
        private int i = 0;

        /**
         * The number of elements read by this reducer.
         **/

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            reducerId = context.getTaskAttemptID().getTaskID().getId();
            numReduceTasks = context.getNumReduceTasks();
            counters = new long[numReduceTasks];
            LdbcDatagen.initializeContext(conf);
            try {
                FileSystem fs = FileSystem.get(conf);
                for (int i = 0; i < (numReduceTasks - 1); ++i) {
                    DataInputStream inputFile = fs
                            .open(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/rank_" + i));
                    counters[i + 1] = inputFile.readLong();
                    inputFile.close();
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
                throw new RuntimeException(e);
            }

            counters[0] = 0;
            for (int i = 1; i < numReduceTasks; ++i) {
                counters[i] += counters[i - 1];
            }
        }

        @Override
        public void reduce(BlockKey key, Iterable<V> valueSet,
                           Context context) throws IOException, InterruptedException {

            for (V v : valueSet) {
                long rank = counters[reducerId] + i;
                context.write(new LongWritable(rank), v);
                i++;
            }
        }
    }


    /**
     * Sorts a hadoop sequence file
     *
     * @param inputFileName  The name of the file to sort.
     * @param outputFileName The name of the sorted file.
     * @throws Exception
     */
    public void run(String inputFileName, String outputFileName) throws Exception {
        int numThreads = conf.getInt("ldbc.snb.datagen.generator.numThreads", 1);

        if (keySetterName != null) {
            conf.set("keySetterClassName", keySetterName);
        }

        /** First Job to sort the key-value pairs and to count the number of elements processed by each reducer.**/
        Job jobSort = Job.getInstance(conf, "Sorting " + inputFileName);

        FileInputFormat.setInputPaths(jobSort, new Path(inputFileName));
        FileOutputFormat
                .setOutputPath(jobSort, new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/rankIntermediate"));

        if (keySetterName != null) {
            jobSort.setMapperClass(HadoopFileRankerSortMapper.class);
        }
        jobSort.setMapOutputKeyClass(K);
        jobSort.setMapOutputValueClass(V);
        jobSort.setOutputKeyClass(BlockKey.class);
        jobSort.setOutputValueClass(V);
        jobSort.setNumReduceTasks(numThreads);
        jobSort.setReducerClass(HadoopFileRankerSortReducer.class);
        jobSort.setJarByClass(V);
        jobSort.setInputFormatClass(SequenceFileInputFormat.class);
        jobSort.setOutputFormatClass(SequenceFileOutputFormat.class);
        InputSampler.Sampler sampler = new InputSampler.RandomSampler(0.1, 1000);
        TotalOrderPartitioner.setPartitionFile(jobSort.getConfiguration(), new Path(inputFileName + "_partition.lst"));
        InputSampler.writePartitionFile(jobSort, sampler);
        jobSort.setPartitionerClass(TotalOrderPartitioner.class);
        if (!jobSort.waitForCompletion(true)) {
            throw new Exception();
        }

        /** Second Job to assign the rank to each element.**/
        Job jobRank = Job.getInstance(conf, "Sorting " + inputFileName);
        FileInputFormat
                .setInputPaths(jobRank, new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/rankIntermediate"));
        FileOutputFormat.setOutputPath(jobRank, new Path(outputFileName));

        jobRank.setMapOutputKeyClass(BlockKey.class);
        jobRank.setMapOutputValueClass(V);
        jobRank.setOutputKeyClass(LongWritable.class);
        jobRank.setOutputValueClass(V);
        jobRank.setSortComparatorClass(BlockKeyComparator.class);
        jobRank.setNumReduceTasks(numThreads);
        jobRank.setReducerClass(HadoopFileRankerFinalReducer.class);
        jobRank.setJarByClass(V);
        jobRank.setInputFormatClass(SequenceFileInputFormat.class);
        jobRank.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobRank.setPartitionerClass(HadoopFileRankerPartitioner.class);
        if (!jobRank.waitForCompletion(true)) {
            throw new Exception();
        }

        try {
            FileSystem fs = FileSystem.get(conf);
            for (int i = 0; i < numThreads; ++i) {
                fs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/rank_" + i), true);
            }
            fs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/rankIntermediate"), true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
