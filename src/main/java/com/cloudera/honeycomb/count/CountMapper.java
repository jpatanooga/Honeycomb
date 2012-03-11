package com.cloudera.honeycomb.count;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;


public class CountMapper extends MapReduceBase implements
    Mapper<LongWritable,Text,Text,IntWritable> {
  
  private JobConf configuration;
  private final Text key = new Text();
  private final IntWritable val = new IntWritable();
  
  private static final Logger logger = Logger.getLogger(CountMapper.class);
  
  /**
   * The close method is called ONCE after all k/v pairs for the split have been
   * processed.
   * 
   */
  public void close() {
    logger.info("CountMapper.close()");

  }
  
  /**
   * 
   * The configure method is where we're going to pull in your metdata for the
   * ETL and VAP processes from the distributed cache.
   * 
   * We'll plugin in the generic POJO code here.
   * 
   */
  
  public void configure(JobConf conf) {
    logger.info("CountMapper.configure()");
    this.configuration = conf;
    
  }
  
  /**
   * This is the method called "per line of input" in the source file "split"
   * 
   * Data will come in as a k/v pair of a "LongWritable" and a "Text" value,
   * which from a text file will be a line of text where the LongWritable
   * represents the byte offset.
   * 
   * 
   * @param inkey
   * @param value
   * @param output
   * @param reporter
   * @throws IOException
   */
  @Override
  public void map(LongWritable inkey, Text value,
      OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
    
    String line = value.toString();
    String[] parts = line.split(" ");
    
    for (int x = 0; x < parts.length; x++) {
      
      key.set(parts[x]);
      val.set(1);
      
      // now that its parsed, we send it through the shuffle for sort, onto
      // reducers
      output.collect(key, val);      
      
    }
    
  }
}
