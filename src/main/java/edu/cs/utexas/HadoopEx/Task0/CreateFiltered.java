package edu.cs.utexas.HadoopEx.Task0;

import edu.cs.utexas.HadoopEx.Task1.LinearRegressionDriver;
import edu.cs.utexas.HadoopEx.Task1.LinearRegressionMapper;
import edu.cs.utexas.HadoopEx.Task1.LinearRegressionReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.ArrayList;

public class CreateFiltered extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CreateFiltered(), args);

        File output = new File(args[1] + "/part-r-00000");
        BufferedReader br = new BufferedReader(new FileReader(output));
        ArrayList<Float> x = new ArrayList<>();
        ArrayList<Float> y = new ArrayList<>();
        String line = br.readLine();
        for (int i = 0; i < 1000 && line != null; i++) {
            String[] split = line.split("\t");
            split = split[1].split(",");
            x.add(Float.parseFloat(split[0]));
            y.add(Float.parseFloat(split[1]));
            line = br.readLine();
        }
        System.out.println(x);
        System.out.println(y);

        br.close();

        System.exit(res);
    }

    /**
     *
     */
    public int run(String args[]) {
        try {
            Configuration conf = new Configuration();

            Job job = new Job(conf, "LinearRegression");
            job.setJarByClass(CreateFiltered.class);

            // specify a Mapper
            job.setMapperClass(FilterMapper.class);

            // specify a Reducer
            job.setReducerClass(FilterReducer.class);

            // specify output types
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            // specify input and output directories
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);

            Path outputPath = new Path(args[1]);
            FileSystem fs = FileSystem.get(outputPath.toUri(), job.getConfiguration());
            fs.delete(outputPath, true);

            return (job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }

    private static class FilterMapper extends Mapper<LongWritable, Text, Object, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] columns = value.toString().split(",");
            if (LinearRegressionMapper.clean(columns)) {
                context.write(new LongWritable(key.get()),
                        new Text(columns[5] + "," + columns[11])
                );
            }
        }
    }

    private static class FilterReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(new LongWritable(key.get()), new Text(value));
            }
        }
    }
}