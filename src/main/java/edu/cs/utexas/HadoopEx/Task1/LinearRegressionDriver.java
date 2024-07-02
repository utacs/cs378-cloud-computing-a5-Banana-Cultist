package edu.cs.utexas.HadoopEx.Task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;


public class LinearRegressionDriver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LinearRegressionDriver(), args);

		if (res == 0) {
			File output = new File(args[1] + "/part-r-00000");
			double[] sums = new double[5];
			BufferedReader br = new BufferedReader(new FileReader(output));
			String line;
			while ((line = br.readLine()) != null) {
				String[] split = line.split("\t");
				sums[Integer.parseInt(split[0])] = Double.parseDouble(split[1]);
			}

			double m;
			double b;

			m = (sums[4]*sums[2]-sums[0]*sums[1])/(sums[4]*sums[3] - sums[3]*sums[3]);
			b = (sums[3]*sums[1]-sums[0]*sums[2])/(sums[4]*sums[3] - sums[3]*sums[3]);
		
			System.out.printf("Parameters:\n\tm: %f\n\tb: %f\n", m, b);
		}

		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			Job job = new Job(conf, "LinearRegression");
			job.setJarByClass(LinearRegressionDriver.class);

			// specify a Mapper
			job.setMapperClass(LinearRegressionMapper.class);

			// specify a Reducer
			job.setReducerClass(LinearRegressionReducer.class);

			// specify output types
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);

			job.setMapOutputValueClass(FloatWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);
			
			return (job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}
