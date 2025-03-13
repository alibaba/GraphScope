package ldbc.snb.datagen.hadoop.serializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class HadoopUpdateStreamSerializer {

    private Configuration conf;

    public static class HadoopUpdateStreamSerializerReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        private OutputStream out;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            int reducerId = Integer.parseInt(conf.get("reducerId"));
            int partitionId = Integer.parseInt(conf.get("partitionId"));
            String streamType = conf.get("streamType");
            try {
                FileSystem fs = FileSystem.get(conf);
                if (Boolean.parseBoolean(conf.get("ldbc.snb.datagen.serializer.compressed"))) {
                    Path outFile = new Path(context.getConfiguration()
                                                   .get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/updateStream_" + reducerId + "_" + partitionId + "_" + streamType + ".csv.gz");
                    out = new GZIPOutputStream(fs.create(outFile));
                } else {
                    Path outFile = new Path(context.getConfiguration()
                                                   .get("ldbc.snb.datagen.serializer.socialNetworkDir") + "/updateStream_" + reducerId + "_" + partitionId + "_" + streamType + ".csv");
                    out = fs.create(outFile);
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> valueSet, Context context)
                throws IOException, InterruptedException {
            for (Text t : valueSet) {
                out.write(t.toString().getBytes("UTF8"));
            }

        }

        protected void cleanup(Context context) {
            try {
                out.close();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public HadoopUpdateStreamSerializer(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    public void run(String inputFileName, int reducer, int partition, String type) throws Exception {

        conf.setInt("reducerId", reducer);
        conf.setInt("partitionId", partition);
        conf.set("streamType", type);

        Job job = Job.getInstance(conf, "Update Stream Serializer");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(HadoopUpdateStreamSerializerReducer.class);
        job.setReducerClass(HadoopUpdateStreamSerializerReducer.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/aux"));
        if (!job.waitForCompletion(true)) {
            throw new Exception();
        }


        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(conf.get("ldbc.snb.datagen.serializer.hadoopDir") + "/aux"), true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}

