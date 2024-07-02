package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.Text;


public class TaxiRate implements Comparable {
    public Text id;
    public float rate;

    public TaxiRate(Text id, float rate) {
        this.id = id;
        this.rate = rate;
    }

    @Override
    public int compareTo(Object o) {
        return Float.compare(rate, ((TaxiRate) o).rate);
    }
}