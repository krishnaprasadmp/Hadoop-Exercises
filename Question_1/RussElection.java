import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;




public class RussElection {

	public static class RussiaMapper1 extends MapReduceBase implements
		Mapper<LongWritable,  
		Text,                 
		Text,               
		LongWritable> {     
		private Text russia_dist = new Text();
		private LongWritable russia_vote = new LongWritable();
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter r) throws IOException {

			try {
				if (key.get() == 0 && value.toString().contains("Name of district"))
					return ;
				else {
					String[] row = value.toString().split(",");
					String district = row[2];
					Long tot = Long.parseLong(row[3]);
					russia_dist.set(district + " ");
					russia_vote.set(tot);
					output.collect(russia_dist, russia_vote);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
	public static class RussiaMapper2 extends MapReduceBase implements
		Mapper<LongWritable,  
		Text,         
		Text,        
		DoubleWritable> {     
		private Text russia_dist = new Text();
		private DoubleWritable russia_vote = new DoubleWritable();
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter r) throws IOException {

			try {
				if (key.get() == 0 && value.toString().contains("Name of district")) /*Some condition satisfying it is header */
					return ;
				else {
					String[] row = value.toString().split(",");
					Long tot = Long.parseLong(row[3]);
					russia_vote.set(tot);
					output.collect(new Text("Mean"), russia_vote);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
	public static class RussiaReducer1 extends MapReduceBase implements
		Reducer<Text,
		LongWritable,
		Text,
		LongWritable> {
		public void reduce(Text key, Iterator<LongWritable> value, OutputCollector<Text, LongWritable> output, Reporter r) throws IOException {


			Long total = 0L;
			while (value.hasNext()) {

				total += value.next().get();
			}

			output.collect(key, new LongWritable(total));

		}
	}
	public static class RussiaReducer2 extends MapReduceBase implements
		Reducer<Text,
		DoubleWritable,
		Text,
		DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> value, OutputCollector<Text, DoubleWritable> output, Reporter r) throws IOException {


			double total_a = 0.0;

			while (value.hasNext()) {

				total_a +=  value.next().get();

			}
			double mean = (total_a / 99.0);

			output.collect(key, new DoubleWritable((mean)));

		}
	}
	public static void main(String[] args) throws Exception {
		String path1 = args[1] + "//a";
		String path2 = args[1] + "//b";
		JobConf job1 = new JobConf(RussElection.class);
		JobConf job2 = new JobConf(RussElection.class);
		job1.setJobName("russia_election");
		job1.setJarByClass(RussElection.class);
		job1.setMapperClass(RussiaMapper1.class);
		job1.setCombinerClass(RussiaReducer1.class);
		job1.setReducerClass(RussiaReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);
		job1.setInputFormat(TextInputFormat.class);
		job1.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(path1));
		job2.setJarByClass(RussElection.class);
		job2.setMapperClass(RussiaMapper2.class);
		job2.setReducerClass(RussiaReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setInputFormat(TextInputFormat.class);
		job2.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(path2));
		job1.setNumReduceTasks(1);
		job2.setNumReduceTasks(1);
		JobClient.runJob(job1);
		JobClient.runJob(job2);


	}

}





