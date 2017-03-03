import java.io.IOException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.*;

import org.apache.hadoop.*;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.util.*;







public class WordOccurence {
	public static class WordMapper1 extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{

	    @Override
	    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
	        String record=value.toString().replaceAll("'$", "").replaceAll("--$","").replaceAll("-$", "").replaceAll("\\)$","").replaceAll("_$","")
	        		.replaceAll("'s$", "").replaceAll("ly$", "").replaceAll("ed$", "").replaceAll("ing$", "").replaceAll("ness$", "")
	        		.replaceAll(",$", "").replaceAll(";$","").replaceAll("!$","").replaceAll(":$","").replaceAll("\\?$","")
	        		 .replaceAll("(^')","").replaceAll("(^\")", "").replaceAll("(^\\()","").replaceAll("^_", "");
	        String data[]=record.split(" ");     
	       
             
	        StringBuilder newKey=new StringBuilder();
	        if(data.length<2)
	            return;
	        for(int i=0;i<data.length-1;i++)  {
	            if(data[i].length()>0&&data[i+1].length()>0)    {
	                newKey=new StringBuilder();
	                if(data[i].compareTo(data[i+1])>0)    {
	                    newKey.append(data[i]).append(" ").append(data[i+1]);
	                }
	                else    {
	                    newKey.append(data[i+1]).append(" ").append(data[i]);
	                }
	                outputCollector.collect(new Text(new String(newKey)),new IntWritable(1));
	            }
	        }
	        newKey=new StringBuilder();
	        if(data[0].length()>0&&data[data.length-1].length()>0)  {
	            if(data[0].compareTo(data[data.length-1])>0)    {
	                newKey.append(data[0]).append(" ").append(data[data.length-1]);
	            }
	            else    {
	                newKey.append(data[data.length-1]).append(" ").append(data[0]);
	            }
	            outputCollector.collect(new Text(new String(newKey)),new IntWritable(1));
	        }
              
	    }
	}
	public static class DecreasingOrderComparator extends WritableComparator{
	    public DecreasingOrderComparator() {
	            super(LongWritable.class, true);
	        }

	        @Override
	        public int compare(WritableComparable n1, WritableComparable n2) {
	            LongWritable value1 = (LongWritable) n1;
	            LongWritable value2 = (LongWritable) n2;

	            int result = value1.get() < value2.get() ? 1 : value1.get() == value2.get() ? 0 : -1;
	            return result;
	        }
	}
	
	public static class WordReducer1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, LongWritable>{

	    @Override
	    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
	        long sum=0;
	        while(values.hasNext()) {
	            sum+=values.next().get();
	        }
	        outputCollector.collect(key,new LongWritable(sum));
	    }
	    
	}
	
	public static class WordMapper2 extends MapReduceBase implements Mapper<LongWritable,Text,LongWritable,Text>{

	    @Override
	    public void map(LongWritable key, Text value, OutputCollector<LongWritable,Text> outputCollector, Reporter r) throws IOException {
	        String data[]=value.toString().split("\\s+");
	        if(data.length>=3)   {
            if(Character.isDigit(data[2].trim().charAt(0)))    {
                outputCollector.collect(new LongWritable(Long.parseLong(data[2].trim())),new Text(data[0]+" "+data[1]));
            }
        }
	        }
	        
	    }
	
	public static class WordReducer2 extends MapReduceBase implements Reducer<LongWritable, Text, Text, LongWritable>{

	    @Override
	    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, LongWritable> outputCollector, Reporter r) throws IOException {
	        while(values.hasNext()) {
	            outputCollector.collect(values.next(),key);
	        }
	    }
	    
	}
	
	public static void main(String[] args) throws Exception{
		JobConf job1=new JobConf(WordOccurence.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path("Output5"));
        job1.setMapperClass(WordMapper1.class);
        job1.setReducerClass(WordReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        JobClient.runJob(job1);
        JobConf job2=new JobConf(WordOccurence.class);
        FileInputFormat.setInputPaths(job2,new Path("Output5"));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        job2.setMapperClass(WordMapper2.class);
        job2.setReducerClass(WordReducer2.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputKeyComparatorClass(DecreasingOrderComparator.class);
        job2.setOutputValueClass(LongWritable.class);
        JobClient.runJob(job2);
        
	}
	

}


