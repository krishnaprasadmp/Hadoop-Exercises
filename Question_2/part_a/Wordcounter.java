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


public class Wordcounter {
	public static class WordMapper1 extends MapReduceBase implements 
	Mapper<LongWritable,
	Text,
	Text,
	IntWritable>
	{  
	    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
	        String record=value.toString().toLowerCase().trim();
	        String data[]=record.split(" ");
	        for(String word:data)   {
	        	word = word.replaceAll("'$", "").replaceAll("--$","").replaceAll("-$", "").replaceAll("\\)$","").replaceAll("_$","");
				word = word.replaceAll("'s$", "").replaceAll("ly$", "").replaceAll("ed$", "").replaceAll("ing$", "").replaceAll("ness$", "");
				word = word.replaceAll(",$", "").replaceAll(";$","").replaceAll("!$","").replaceAll(":$","").replaceAll("\\?$","");
				word = word.replaceAll("(^')","").replaceAll("(^\")", "").replaceAll("(^\\()","").replaceAll("^_", "");
	            outputCollector.collect(new Text(word),new IntWritable(1));
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
	public static class WordComparator extends WritableComparator {

	    public WordComparator() {
	        super(Text.class);
	    }

	    @Override
	    public int compare(byte[] p1, int q1, int r1,
	            byte[] p2, int q2, int r2) {

	        String val1 = ByteBuffer.wrap(p1, q1, r1).toString();
	        String val2 = ByteBuffer.wrap(p2, q2, r2).toString();

	        return val1.compareTo(val2)*(-1);
	    }
	}
	public static class WordReducer1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, LongWritable>{
        
	    @Override
	    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
	        long total=0;
	        while(values.hasNext()) {
	            total+=values.next().get();
	        }
	        outputCollector.collect(key,new LongWritable(total));
	    }
	    
	}
	public static class WordMapper2 extends MapReduceBase implements Mapper<LongWritable,Text,LongWritable,Text>{

	    @Override
	    public void map(LongWritable key, Text value, OutputCollector<LongWritable,Text> outputCollector, Reporter r) throws IOException {
	        String record[]=value.toString().split("\\s+");
	        
	        if(record.length>=2)   {
	            if(Character.isDigit(record[1].trim().charAt(0)))    {
	                outputCollector.collect(new LongWritable(Long.parseLong(record[1].trim())),new Text(record[0]));
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
		JobConf job1=new JobConf(Wordcounter.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path("Output1"));
        job1.setMapperClass(WordMapper1.class);
        job1.setReducerClass(WordReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        JobClient.runJob(job1);
        JobConf job2=new JobConf(Wordcounter.class);
        FileInputFormat.setInputPaths(job2,new Path("Output1"));
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
