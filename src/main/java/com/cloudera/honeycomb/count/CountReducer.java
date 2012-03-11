package com.cloudera.honeycomb.count;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CountReducer extends MapReduceBase implements
    Reducer<Text,IntWritable,Text,Text> {
  
  private JobConf configuration;
  
  /**
   * This method is called once before we process any kv pairs to setup any
   * small amounts of state we might need.
   * 
   */
  @Override
  public void configure(JobConf job) {
    
    this.configuration = job;
    
  } // configure()
  
  /**
   * This is the actual reduce function; Unlike the map function, it is only
   * called once per key.
   * 
   */
  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
    
    int sum = 0;
    IntWritable current_rec = null;
    Text out_val = new Text();
    
    /*
     * Let's loop through all of the k/v pairs for this key and process them
     */
    while (values.hasNext()) {
      
      current_rec = values.next();
      sum += current_rec.get();
      
    } // while
    
    out_val.set(String.valueOf(sum));    
    output.collect(key, out_val);
    
  } // reduce
  
}
