package LastProject;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TestNewCombinerGrouping.Combiner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class uvtest {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private String d = null;
		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String date[]=conf.getStrings("date");
			d = date[2];
		}
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			FileSplit filesplit = (FileSplit) context.getInputSplit();
			String path[] = filesplit.getPath().toString().split("/");
			String fpath = path[path.length-2];
			String mpath = path[path.length-3];
			if(d.contains("day")){
				if(fpath.equals(d.substring(0, 2))){
					String line = value.toString();			
					String ip = line.split(" ")[0];
					context.write(new Text(ip), new IntWritable(1));
				}
			}else if(d.contains("week")){
				int week = Integer.parseInt(d.substring(0, 1));
				if(Integer.parseInt(fpath)<(week*7) && Integer.parseInt(fpath)>((week-1)*7)){
					String line = value.toString();			
					String ip = line.split(" ")[0];
					context.write(new Text(ip), new IntWritable(1));
				}
				
			}else if(d.contains("month")){
				if(mpath.split("-").equals(d.substring(0, 2))){
					String line = value.toString();			
					String ip = line.split(" ")[0];
					context.write(new Text(ip), new IntWritable(1));
				}
			}
			

		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private int sum=0;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {			
				sum++;
		}
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			Counter counter=context.getCounter("c1", "c2");
			System.out.println("sum:"+sum);
			counter.increment(sum);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setStrings("date", args);		
		Job job = new Job(conf);
		job.setJobName("uv");
		job.setJarByClass(uvtest.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean status = job.waitForCompletion(true);
		if(status){
			Counters cs = job.getCounters();
			Counter counter=cs.findCounter("c1", "c2");
			System.out.println(args[2]+"'s UV---"+counter.getValue());
		}
		
		
		
	}
}
