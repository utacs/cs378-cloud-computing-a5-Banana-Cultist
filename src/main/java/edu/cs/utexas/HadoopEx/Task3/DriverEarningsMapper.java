package edu.cs.utexas.HadoopEx.Task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DriverEarningsMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] columns = value.toString().split(",");
        if (columns.length == 17) {
            String driverID = columns[1];
            int duration = 0;
            float earnings = 0f;
            boolean valid = false;
            try {
                duration = Integer.parseInt(columns[4]);
                earnings = Float.parseFloat(columns[16]);
                valid = duration > 0 && earnings <= 500f;
            } catch (NumberFormatException e) {}

            if (valid) {
                context.write(new Text(driverID), new Text(duration + "," + earnings));
            }
        }
    }
}
