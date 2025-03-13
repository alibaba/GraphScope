package ldbc.snb.datagen.hadoop.miscjob.keychanger;

import ldbc.snb.datagen.hadoop.key.TupleKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class HadoopFileKeyChanger {

    private String keySetterName;
    private Configuration conf;
    private Class<?> K;
    private Class<?> V;

    public interface KeySetter<K> {
        K getKey(Object object);
    }


    public HadoopFileKeyChanger(Configuration conf, Class<?> K, Class<?> V, String keySetterName) {
        this.keySetterName = keySetterName;
        this.conf = conf;
        this.K = K;
        this.V = V;
    }

    public static class HadoopFileKeyChangerReducer<K, V> extends Reducer<K, V, TupleKey, V> {

        private KeySetter<TupleKey> keySetter;

        @Override
        public void setup(Context context) {
            try {
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
        public void reduce(K key, Iterable<V> valueSet,
                           Context context) throws IOException, InterruptedException {
            for (V v : valueSet) {
                context.write(keySetter.getKey(v), v);
            }
        }
    }

    public void run(String inputFileName, String outputFileName) throws Exception {

        int numThreads = conf.getInt("ldbc.snb.datagen.generator.numThreads", 1);
        System.out.println("***************" + numThreads);
        conf.set("keySetterClassName", keySetterName);

        /** First Job to sort the key-value pairs and to count the number of elements processed by each reducer.**/
        Job job = Job.getInstance(conf, "Sorting " + inputFileName);

        FileInputFormat.setInputPaths(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));

        job.setMapOutputKeyClass(K);
        job.setMapOutputValueClass(V);
        job.setOutputKeyClass(TupleKey.class);
        job.setOutputValueClass(V);
        job.setNumReduceTasks(numThreads);
        job.setReducerClass(HadoopFileKeyChangerReducer.class);
        job.setJarByClass(V);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        if (!job.waitForCompletion(true)) {
            throw new Exception();
        }
    }
}
