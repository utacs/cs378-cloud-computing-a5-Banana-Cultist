package edu.cs.utexas.HadoopEx.Task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import edu.cs.utexas.HadoopEx.Task1.LinearRegressionMapper;

import java.io.IOException;
import java.util.ArrayList;

public class GradientDescentMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] columns = value.toString().split(",");
		if (LinearRegressionMapper.clean(columns)) {
			Float x = Float.parseFloat(columns[5]);
			Float y = Float.parseFloat(columns[11]);

			Configuration conf = context.getConfiguration();


			Double m = Double.parseDouble(conf.get("m"));
			Double b = Double.parseDouble(conf.get("b"));

			Double partial_m = -x * (y - (m * x + b));
			Double partial_b = -1 * (y - (m * x + b));

			// m
			context.write(new IntWritable(0), new DoubleWritable(partial_m));

			//b
			context.write(new IntWritable(1), new DoubleWritable(partial_b));

			// calculate cost
			Double cost = Math.pow((y - (m * x + b)), 2);

			// write cost to index 2
			context.write(new IntWritable(2), new DoubleWritable(cost));
		}
	}
}
