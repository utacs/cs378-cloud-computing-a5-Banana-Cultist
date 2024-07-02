package edu.cs.utexas.HadoopEx.Task3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class DriverEarningsReducer extends  Reducer<Text, Text, Text, FloatWritable> {
    public void reduce(Text driverID, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int duration = 0;
        float earnings = 0;

        for (Text value : values) {
            String[] vals = value.toString().split(",");
            duration += Integer.parseInt(vals[0]);
            earnings += Float.parseFloat(vals[1]);
            earnings = Math.round(earnings * 100) / 100f;
        }

        context.write(driverID, new FloatWritable(earnings / (duration / 60f)));
    }
}
