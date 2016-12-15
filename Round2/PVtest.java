package LastProject;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PVtest {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private int sum = 0;
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			sum++;	
		}
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			Counter counter = context.getCounter("c1", "c2");
			counter.increment(sum);
		}
	}
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf);
		job.setJarByClass(PVtest.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		try {
			boolean status = job.waitForCompletion(true);
			if(status){
				Counters cs = job.getCounters();
				Counter counter = cs.findCounter("c1", "c2");
				System.out.println("PV---"+counter.getValue());
			}
		} catch (ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		
		
		
	}
}
