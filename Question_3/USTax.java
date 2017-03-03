

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.*;

import org.apache.hadoop.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.*;



public class USTax {
	private static class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
		String state_name;
		String agi_stub;

		public CompositeGroupKey() {

		}

		public CompositeGroupKey(String state_name, String agi_stub) {
			this.state_name = state_name;
			this.agi_stub = agi_stub;
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			this.state_name = WritableUtils.readString(input);
			this.agi_stub = WritableUtils.readString(input);

		}

		@Override
		public void write(DataOutput output) throws IOException {
			WritableUtils.writeString(output, state_name);
			WritableUtils.writeString(output, agi_stub);

		}

		@Override
		public int compareTo(CompositeGroupKey p) {
			if (p == null) {
				return 0;
			}
			int count = state_name.compareTo(p.state_name);
			return count == 0 ? agi_stub.compareTo(p.agi_stub) : count;
		}
		public String toString() {
			return state_name.toString() + ":" + agi_stub.toString();
		}

	}
	public static class TaxMapper extends MapReduceBase implements Mapper<LongWritable, Text, CompositeGroupKey, IntWritable>  {
		private IntWritable ag_inc = new IntWritable();
		public void map(LongWritable key, Text value, OutputCollector<CompositeGroupKey, IntWritable> output, Reporter r)
		throws IOException {
			try {
				if (key.get() == 0 && value.toString().contains("N1")) // we can use regex
					return ;
				else {
					String[] row = value.toString().split(",");
					String inc = row[4];
					double inc_double = Double.parseDouble(inc);
					int inc_tot = (int) inc_double;
					ag_inc.set(inc_tot);
					CompositeGroupKey keypair = new CompositeGroupKey(row[1], row[3]);
					output.collect(keypair, ag_inc);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	public static class TaxReducer extends MapReduceBase implements Reducer<CompositeGroupKey, IntWritable, CompositeGroupKey, IntWritable> {
		public void reduce(CompositeGroupKey key, Iterator<IntWritable> value,
		                   OutputCollector<CompositeGroupKey, IntWritable> output, Reporter r) throws IOException {

			int sum = 0;
			while (value.hasNext()) {
				sum += value.next().get();
			}

			output.collect(key, new IntWritable(sum));

		}

	}
	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf(USTax.class);
		job.setJobName("tax");
		job.setJarByClass(USTax.class);
		job.setMapperClass(TaxMapper.class);
		job.setCombinerClass(TaxReducer.class);
		job.setReducerClass(TaxReducer.class);
		job.setOutputKeyClass(CompositeGroupKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);
		JobClient.runJob(job);



	}

}




