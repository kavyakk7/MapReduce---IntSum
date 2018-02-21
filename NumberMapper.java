package hadoop;

import java.io.*;
import java.util.StringTokenizer;

//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//import org.hsqldb.Tokenizer;

public class NumberMapper extends Mapper <LongWritable, Text, Text, IntWritable> 
    {
        
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
        	int sum;
            StringTokenizer itr=new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                sum+=Integer.parseInt(itr.nextToken());
            }
            context.write(new Text("sum"),new IntWritable(sum));
        }
    }