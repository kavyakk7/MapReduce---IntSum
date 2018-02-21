package hadoop;

import java.io.*;
//import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntSumDriver {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
      // TODO Auto-generated method stub
          Configuration conf=new Configuration();
          /*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
          if(otherArgs.length!=2)
          {
              System.err.println("Error");
              System.exit(2);
          }*/
          Job job=new Job(conf, "number sum");
          job.setJarByClass(IntSumDriver.class);
          job.setMapperClass(IntSumMapper.class);
          job.setReducerClass(IntSumReducer.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);
          FileInputFormat.addInputPath(job, new Path(args[0]));
          FileOutputFormat.setOutputPath(job, new Path(args[1]));
          System.exit(job.waitForCompletion(true)?0:1);
  }

}

class IntSumMapper extends Mapper <LongWritable, Text, Text, IntWritable> 
{
    
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
    {
    	int sum = 0;
        StringTokenizer itr=new StringTokenizer(value.toString());
        while(itr.hasMoreTokens())
        {
            sum+=Integer.parseInt(itr.nextToken());
        }
        context.write(new Text("sum"),new IntWritable(sum));
    }
}

class IntSumReducer extends Reducer <Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text key,Iterable<IntWritable> values, Context context)throws IOException, InterruptedException
    {
        int sum=0;
        for(IntWritable value:values)
            {
                sum+=value.get();
            }
        context.write(key,new IntWritable(sum));
    }
}