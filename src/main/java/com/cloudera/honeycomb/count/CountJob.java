package com.cloudera.honeycomb.count;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountJob extends Configured implements Tool {
  
  /**
   * The run method is the method called by the Map Reduce system to construct
   * and drive the job.
   * 
   */
  @Override
  public int run(String[] args) throws Exception {
        
    JobConf conf = new JobConf(getConf(), CountJob.class);
    conf.setJobName("CountJob");
    
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);
    
    conf.setMapperClass(CountMapper.class);
    conf.setReducerClass(CountReducer.class);
    
    /*
     * This section of code is meant to parse command line arguments and load
     * them into the distributed configuration system so that all map / reduce
     * tasks can see them.
     */
    List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
          
        } else if ("-r".equals(args[i])) {
          
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
          
        } else {
          
          other_args.add(args[i]);
          
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
            + args[i - 1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: "
          + other_args.size() + " instead of 2.");
      return printUsage();
    }
    
    /*
     * Here we're setting our input and output formats
     */
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setCompressMapOutput(true);
    
    FileInputFormat.setInputPaths(conf, other_args.get(0));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
    
    // and finally we run the job
    JobClient.runJob(conf);
    
    return 0;
  }
  
  static int printUsage() {
    System.out
        .println("CountJob [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
  /**
   * Method to crank up the ToolRunner app, load the configuration, and drive
   * the job creation method "run()"
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    
    int res = ToolRunner.run(new Configuration(), new CountJob(), args);
    System.exit(res);
    
  }
  
}
