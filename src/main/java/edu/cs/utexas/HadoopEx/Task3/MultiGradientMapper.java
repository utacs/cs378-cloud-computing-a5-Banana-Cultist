package edu.cs.utexas.HadoopEx.Task3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.cs.utexas.HadoopEx.Task1.LinearRegressionMapper;

public class MultiGradientMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] columns = value.toString().split(",");
		if (LinearRegressionMapper.clean(columns)) {
			Float x0 = Float.parseFloat(columns[4]);
            Float x1 = Float.parseFloat(columns[5]);
            Float x2 = Float.parseFloat(columns[11]);
            Float x3 = Float.parseFloat(columns[15]);
			Float y = Float.parseFloat(columns[16]);

			Configuration conf = context.getConfiguration();


			Double m0 = Double.parseDouble(conf.get("m0"));
            Double m1 = Double.parseDouble(conf.get("m1"));
            Double m2 = Double.parseDouble(conf.get("m2"));
            Double m3 = Double.parseDouble(conf.get("m3"));
			Double b = Double.parseDouble(conf.get("b"));

			Double partial_m0 = -x0 * (y - ((m0 * x0) + (m1 * x1) + (m2 * x2) + (m3 * x3) + b));
            Double partial_m1 = -x1 * (y - ((m0 * x0) + (m1 * x1) + (m2 * x2) + (m3 * x3) + b));
            Double partial_m2 = -x2 * (y - ((m0 * x0) + (m1 * x1) + (m2 * x2) + (m3 * x3) + b));
            Double partial_m3 = -x3 * (y - ((m0 * x0) + (m1 * x1) + (m2 * x2) + (m3 * x3) + b));
			Double partial_b = -1 * (y - ((m0 * x0) + (m1 * x1) + (m2 * x2) + (m3 * x3) + b));

			// m0-4
			context.write(new IntWritable(0), new DoubleWritable(partial_m0));
			context.write(new IntWritable(1), new DoubleWritable(partial_m1));
			context.write(new IntWritable(2), new DoubleWritable(partial_m2));
			context.write(new IntWritable(3), new DoubleWritable(partial_m3));

			//b
			context.write(new IntWritable(4), new DoubleWritable(partial_b));

			// calculate cost
			Double cost = Math.pow((y - ((m0 * x0) + (m1 * x1) + (m2 * x2) + (m3 * x3) + b)), 2);

			// write cost to index 5
			context.write(new IntWritable(5), new DoubleWritable(cost));
		}
	}
}
