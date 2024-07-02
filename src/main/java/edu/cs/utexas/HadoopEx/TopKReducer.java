package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;


public class TopKReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

    private PriorityQueue<TaxiRate> pq = new PriorityQueue<>(TopKMapper.K);

   public void setup(Context context) {
       pq = new PriorityQueue<>(TopKMapper.K);
   }

    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
   public void reduce(Text key, Iterable<FloatWritable> values, Context context)
           throws IOException, InterruptedException {

       // size of values is 1 because key only has one distinct value
       for (FloatWritable value : values) {
           pq.add(new TaxiRate(new Text(key), value.get()));
       }

       // keep the priorityQueue size <= heapSize
       while (pq.size() > TopKMapper.K) {
           pq.poll();
       }
   }


    public void cleanup(Context context) throws IOException, InterruptedException {
        List<TaxiRate> values = new ArrayList<>(TopKMapper.K);

        while (!pq.isEmpty()) {
            values.add(pq.poll());
        }

        // reverse so they are ordered in descending order
        Collections.reverse(values);

        for (TaxiRate value : values) {
            context.write(value.id, new FloatWritable(value.rate));
        }
    }
}
