package LastProject;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class iosAndAndroid {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Pattern pattern = Pattern.compile("(\\d+.\\d+.\\d+.\\d+) [^ ]* [^ ]* \\[([^ ]* [^ ]*)\\] \"[^ ]+ ([^ ]+) .*\" \\d+ \\d+ \"(.*)\" \"(.*)\"");
			Matcher matcher = pattern.matcher(line);
			if(matcher.find()){
				String ip = matcher.group(1);
				String agent = matcher.group(5);
				context.write(new Text(ip), new Text(agent));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		private int sum_ip=0;
		private int sum_ios=0;
		private int sum_and=0;
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			String str="";
			for(Text value:values){
				String agent=value.toString().toLowerCase();
				str+=agent;
			}
			if(str.contains("ios")){
				sum_ios++;
			}else if(str.contains("android")){
				sum_and++;
			}
			sum_ip++;
			context.write(key, new Text(str));
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Counter counter1 = context.getCounter("sum", "ip");
			Counter counter2 = context.getCounter("sum", "ios");
			Counter counter3 = context.getCounter("sum", "and");
			counter1.increment(sum_ip);
			counter2.increment(sum_ios);
			counter3.increment(sum_and);
			context.write(new Text("\n"), new Text("c_ip:"+counter1.getValue()+"  c_ios"+counter2.getValue()+"  c_android"+counter3.getValue()));
		
		}
	}
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(iosAndAndroid.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		try {
			boolean statu = job.waitForCompletion(true);
			if(statu){
				Counters cs = job.getCounters();
				Counter ip= cs.findCounter("sum", "ip");
				Counter ios = cs.findCounter("sum", "ios");
				Counter android = cs.findCounter("sum", "and");
				System.out.println("ip:"+ip.getValue()+"  ios:"+ios.getValue()+"  android:"+android.getValue());
			}
		} catch (ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
}
