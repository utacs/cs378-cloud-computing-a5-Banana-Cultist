package edu.cs.utexas.HadoopEx.Task3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MultiGradientDriver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MultiGradientDriver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.set("m0", "0.1");
        conf.set("m1", "0.1");
        conf.set("m2", "0.1");
        conf.set("m3", "0.1");
		conf.set("b", "0.1");

		File results = null;
		BufferedWriter bw = null;

		int max_iters = 100;
		int num_iter = 1;
		double learning_rate = 0.001;

		while (num_iter < max_iters) {
			// Reset job configurations for each iteration
			Job job = Job.getInstance(conf, "MultiGradient");

			job.setJarByClass(MultiGradientDriver.class);

			// specify a Mapper
			job.setMapperClass(MultiGradientMapper.class);

			// specify a Reducer
			job.setReducerClass(MultiGradientReducer.class);

			// specify output types
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			Path outPath = new Path(args[1] + '/' + num_iter);
			FileOutputFormat.setOutputPath(job, outPath);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.waitForCompletion(false);

			// read job output
			File output = new File(outPath + "/part-r-00000");
			BufferedReader br = new BufferedReader(new FileReader(output));
			String line = br.readLine();
			double cost = 0, m0 = 0, m1 = 0, m2 = 0, m3 = 0, b = 0;
			while (line != null) {
				String[] split = line.split("\t");
				double parsed = Double.parseDouble(split[1]);
				switch(split[0]) {
					case "0":
						m1 = parsed;
						break;
					case "1":
						m1 = parsed;
						break;
					case "2":
						m2 = parsed;
						break;
                    case "3":
                        m3 = parsed;
                        break;
                    case "4":
                        b = parsed;
                    case "5":
                        cost = parsed;
				}

				line = br.readLine();
			}
			br.close();

			// calculate new parameters on learning rate
            m0 = Double.parseDouble(conf.get("m0")) - learning_rate * m0;
			m1 = Double.parseDouble(conf.get("m1")) - learning_rate * m1;
            m2 = Double.parseDouble(conf.get("m2")) - learning_rate * m2;
            m3 = Double.parseDouble(conf.get("m3")) - learning_rate * m3;
			b = Double.parseDouble(conf.get("b")) - learning_rate * b;

			// update parameters
			conf.set("m0", "" + m0);
			conf.set("m1", "" + m1);
			conf.set("m2", "" + m2);
			conf.set("m3", "" + m3);
			conf.set("b", "" + b);

			// write to output file
			String writeOutput = String.format("Cost: %f\n\tm: %f\n\tb: %f\n", cost, m0, m1, m2, m3, b);

			if (bw == null) {
				results = new File(args[1] + "/" + args[2]);
				bw = new BufferedWriter(new FileWriter(results));
			}

			bw.write(writeOutput);
			bw.flush();

			// delete output directory (for cleanliness)
			FileSystem.get(outPath.toUri(), job.getConfiguration()).delete(outPath, true);

			System.out.println("Iteration: " + num_iter);
			System.out.println(writeOutput);
			num_iter++;
		}

		bw.close();
		return 1;
	}
}

