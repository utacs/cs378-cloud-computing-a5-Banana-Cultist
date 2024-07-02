package edu.cs.utexas.HadoopEx.Task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HourErrorMapper extends Mapper<Object, Text, IntWritable, LongWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] columns = value.toString().split(",");
		if (columns.length == 17) {
			try {
				boolean[] gpsError = checkGPSError(columns);
				if (gpsError[0]) {
					String[] datetimePickup = columns[2].split(" ");
					String[] timePickup = datetimePickup[1].split(":");
					int pickupHour = Integer.parseInt(timePickup[0]);
					//System.out.println("\nERROR AT HOUR: " + pickupHour + "\n");
					context.write(new IntWritable(pickupHour + 1), new LongWritable(1));
				}
				if (gpsError[1]) {
					String[] datetimeDropoff = columns[3].split(" ");				
					String[] timeDropoff = datetimeDropoff[1].split(":");
					int dropoffHour = Integer.parseInt(timeDropoff[0]);
					//System.out.println("\nERROR AT HOUR: " + dropoffHour + "\n");
					context.write(new IntWritable(dropoffHour + 1), new LongWritable(1));
				}
			} catch (Exception e) {

			}
		}
	}
	
	public static boolean[] checkGPSError(String[] columns) {
		boolean[] gpsError = new boolean[] {false, false};
		try {
			if (Float.parseFloat(columns[6]) == 0f || Float.parseFloat(columns[7]) == 0f) {
				gpsError[0] = true;
			}
		} catch (NumberFormatException e) {
			gpsError[0] = true;
		}

		try {
			if (Float.parseFloat(columns[8]) == 0f || Float.parseFloat(columns[9]) == 0f) {
				gpsError[1] = true;
			}
		} catch (NumberFormatException e) {
			gpsError[1] = true;
		}
		
		return gpsError;
	}
}
