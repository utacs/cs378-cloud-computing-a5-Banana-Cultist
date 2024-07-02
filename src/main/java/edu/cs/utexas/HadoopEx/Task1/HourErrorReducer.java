package edu.cs.utexas.HadoopEx.Task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class HourErrorReducer extends  Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

   public void reduce(IntWritable hour, Iterable<LongWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       long errors = 0;
       
       for (LongWritable value : values) {
           errors += value.get();
       }
       
       context.write(hour, new LongWritable(errors));
   }
}
