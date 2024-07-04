package edu.cs.utexas.HadoopEx.Task3;

//import edu.cs.utexas.HadoopEx.Task2.TaxiErrorDriver;
import edu.cs.utexas.HadoopEx.TopKMapper;
import edu.cs.utexas.HadoopEx.TopKReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DriverEarningsDriver extends Configured implements Tool {

    /**
     *
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DriverEarningsDriver(), args);
        System.exit(res);
    }

    /**
     *
     */
    public int run(String args[]) {
        try {
            Configuration conf = new Configuration();

            Job job = new Job(conf, "DriverEarnings");
            job.setJarByClass(DriverEarningsDriver.class);

            // specify a Mapper
            job.setMapperClass(DriverEarningsMapper.class);

            // specify a Reducer
            job.setReducerClass(DriverEarningsReducer.class);

            // specify output types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // specify input and output directories
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            // delete previous output
            Path outputPathMid = new Path(args[1]);
            FileSystem fs = FileSystem.get(outputPathMid.toUri(), job.getConfiguration());
            fs.delete(outputPathMid, true);

            FileOutputFormat.setOutputPath(job, outputPathMid);
            job.setOutputFormatClass(TextOutputFormat.class);

            if (!job.waitForCompletion(true)) {
                return 1;
            }
            Job job2 = new Job(conf, "TopDriverEarnings");
            job2.setJarByClass(DriverEarningsDriver.class);

            // specify a Mapper
            TopKMapper.K = 10;

            job2.setMapperClass(TopKMapper.class);
            job2.setReducerClass(TopKReducer.class);

            // specify output types
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(FloatWritable.class);

            // set the number of reducer to 1
            job2.setNumReduceTasks(1);

            // specify input and output directories
            FileInputFormat.addInputPath(job2, outputPathMid);
            job2.setInputFormatClass(KeyValueTextInputFormat.class);

            // delete previous output
            Path outputPathFinal = new Path(args[2]);
            fs = FileSystem.get(outputPathFinal.toUri(), job.getConfiguration());
            fs.delete(outputPathFinal, true);

            FileOutputFormat.setOutputPath(job2, outputPathFinal);
            job2.setOutputFormatClass(TextOutputFormat.class);

            return (job2.waitForCompletion(true) ? 0 : 1);

        } catch (InterruptedException | ClassNotFoundException | IOException e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }
}
