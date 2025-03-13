package ldbc.snb.datagen.hadoop.serializer;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.LdbcDatagen;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.hadoop.HadoopBlockMapper;
import ldbc.snb.datagen.hadoop.HadoopTuplePartitioner;
import ldbc.snb.datagen.hadoop.key.TupleKey;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.UpdateEventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class HadoopPersonSerializer {

    private Configuration conf;

    public static class HadoopDynamicPersonSerializerReducer extends Reducer<TupleKey, Person, LongWritable, Person> {

        private int reducerId;
        /**
         * The id of the reducer.
         **/
        private DynamicPersonSerializer dynamicPersonSerializer_;
        /**
         * The person serializer
         **/
        private UpdateEventSerializer updateSerializer_;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            reducerId = context.getTaskAttemptID().getTaskID().getId();
            LdbcDatagen.initializeContext(conf);
            try {
                dynamicPersonSerializer_ = (DynamicPersonSerializer) Class
                        .forName(conf.get("ldbc.snb.datagen.serializer.dynamicPersonSerializer")).newInstance();
                dynamicPersonSerializer_.initialize(conf, reducerId);
                if (DatagenParams.updateStreams) {
                    updateSerializer_ = new UpdateEventSerializer(conf, DatagenParams.hadoopDir + "/temp_updateStream_person_" + reducerId, reducerId, DatagenParams.numUpdatePartitions);
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reduce(TupleKey key, Iterable<Person> valueSet, Context context)
                throws IOException {
            for (Person p : valueSet) {
                if (p.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
                    dynamicPersonSerializer_.export(p);
                } else {
                    updateSerializer_.export(p);
                    updateSerializer_.changePartition();
                }

                for (Knows k : p.knows()) {
                    if (k.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
                        dynamicPersonSerializer_.export(p, k);
                    }
                }
            }

        }

        @Override
        protected void cleanup(Context context) {
            dynamicPersonSerializer_.close();
            if (DatagenParams.updateStreams) {
                try {
                    updateSerializer_.close();
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        }
    }

    public HadoopPersonSerializer(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    public void run(String inputFileName) throws Exception {

        FileSystem fs = FileSystem.get(conf);

        int numThreads = Integer.parseInt(conf.get("ldbc.snb.datagen.generator.numThreads"));
        Job job = Job.getInstance(conf, "Person Serializer");
        job.setMapOutputKeyClass(TupleKey.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Person.class);
        job.setJarByClass(HadoopBlockMapper.class);
        job.setReducerClass(HadoopDynamicPersonSerializerReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setPartitionerClass(HadoopTuplePartitioner.class);

        FileInputFormat.setInputPaths(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/aux"));
        if (!job.waitForCompletion(true)) {
            throw new Exception();
        }

        try {
            fs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/aux"), true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
