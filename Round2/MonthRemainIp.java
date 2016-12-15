package LastProject;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import LastProject.uvtest.Map;
import LastProject.uvtest.Reduce;

public class MonthRemainIp {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private String day1=null;
		private String day2=null;
		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String str[] = conf.getStrings("day");
			day1 = str[2];
			day2 = str[3];
		}
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			FileSplit filesplit = (FileSplit) context.getInputSplit();
			String path[]=filesplit.getPath().toString().split("/");
			String filename=path[path.length-3];
			if(filename.equals(day1)){
				String ip = value.toString().split(" ")[0];
				context.write(new Text(ip), new Text("a"));
			}else{
				String ip = value.toString().split(" ")[0];
				context.write(new Text(ip), new Text("b"));
			}
			
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		private int suma=0;
		private int sumb=0;
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			HashSet<String>set=new HashSet<>();
			for(Text value:values){
				set.add(value.toString());
			}
			if(set.contains("a"))
				suma++;
			if(set.contains("a") && set.contains("b"))
				sumb++;
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Counter counter = context.getCounter("c1", "suma");
			Counter counter2 = context.getCounter("c1", "sumb");
			counter.increment(suma);
			counter2.increment(sumb);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setStrings("day", args);		
		Job job = new Job(conf);
		job.setJobName("MonthRemainIp");
		job.setJarByClass(MonthRemainIp.class);
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
			Counter counter=cs.findCounter("c1", "suma");
			Counter counter2=cs.findCounter("c1", "sumb");
			System.out.println(args[2]+"day and "+args[3]+"month's RemainIp---"+(double)counter.getValue()/counter2.getValue());
		}
	}
		
}
