package edu.cs.utexas.HadoopEx.Task2;

import edu.cs.utexas.HadoopEx.Task1.HourErrorMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TaxiErrorMapper extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] columns = value.toString().split(",");
		if (columns.length == 17) {
			boolean[] gpsError = HourErrorMapper.checkGPSError(columns);
			if (gpsError[0] || gpsError[1]) {
				context.write(new Text(columns[0]), new IntWritable(1));
			} else {
				context.write(new Text(columns[0]), new IntWritable(0));
			}
		}
	}
}
