package edu.cs.utexas.HadoopEx.Task2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TaxiErrorReducer extends  Reducer<Text, IntWritable, Text, FloatWritable> {


    public void reduce(Text medallion, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        int errors = 0;
        int total = 0;
        
        for (IntWritable value : values) {
            errors += value.get();
            total++;
        }
        
        context.write(medallion, new FloatWritable((float) errors / total));
    }
}
