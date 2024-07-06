package edu.cs.utexas.HadoopEx;

import edu.cs.utexas.HadoopEx.Task1.LinearRegressionDriver;
import edu.cs.utexas.HadoopEx.Task1.LinearRegressionMapper;
import edu.cs.utexas.HadoopEx.Task1.LinearRegressionReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.*;
import java.io.IOException;
import java.util.ArrayList;

public class JobReader extends Configured implements Tool {
    private static ArrayList<String> lines = new ArrayList<>();

    static class JobReaderMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(new LongWritable(key.get()), new Text(value));
        }
    }

    static class JobReaderReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text val : values) {
                lines.add(val.toString());
            }
        }
    }

    public static List<String> getJobOutput(String read) throws Exception {
        lines.clear();
        ToolRunner.run(new Configuration(), new JobReader(), new String[] {read});
        return lines;
    }

    public int run(String args[]) {
        try {
            Configuration conf = new Configuration();

            Job job = new Job(conf, "JobReader");
            job.setJarByClass(JobReader.class);

            // specify a Mapper
            job.setMapperClass(JobReaderMapper.class);

            // specify a Reducer
            job.setReducerClass(JobReaderReducer.class);
            job.setNumReduceTasks(1);

            // specify output types
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            // specify input and output directories
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            Path outPath = new Path("temporaryJobReader");
            FileOutputFormat.setOutputPath(job, outPath);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);

            FileSystem.get(outPath.toUri(), job.getConfiguration()).delete(outPath, true);

        } catch (Exception e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }

        return 1;
    }
}
