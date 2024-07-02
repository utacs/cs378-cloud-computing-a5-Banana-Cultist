package edu.cs.utexas.HadoopEx.Task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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
			ArrayList<Double> parameters = Parameters.getParameters();
			Double m = parameters.get(0);
			Double b = parameters.get(1);

			Double partial_m = -x * (y - (m * x + b));
			Double partial_b = -1 * (y - (m * x + b));

			// m
			context.write(new IntWritable(0), new DoubleWritable(partial_m));

			//b
			context.write(new IntWritable(1), new DoubleWritable(partial_b));
		}
	}
}
