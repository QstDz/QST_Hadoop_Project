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

public class baidu {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Pattern pattern = Pattern.compile("(\\d+.\\d+.\\d+.\\d+) [^ ]* [^ ]* \\[([^ ]* [^ ]*)\\] \"[^ ]+ ([^ ]+) .*\" \\d+ \\d+ \"(.*)\" \"(.*)\"");
			Matcher matcher = pattern.matcher(line);
			if(matcher.find()){
				String url = matcher.group(3);
				String refer = matcher.group(4);
				context.write(new Text(url), new Text(refer));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		private int sum_url=0;
		private int sum_baidu=0;
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			for(Text value:values){
				String refer=value.toString();
				if(refer.contains("baidu")){
					sum_baidu++;
					context.write(key, new Text(sum_baidu+"  "+refer));
				}
			}
			sum_url++;
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Counter counter1 = context.getCounter("sum", "url");
			Counter counter2 = context.getCounter("sum", "baidu");
			counter1.increment(sum_url);
			counter2.increment(sum_baidu);
		
		}
	}
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(baidu.class);
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
				Counter url = cs.findCounter("sum", "url");
				Counter baidu = cs.findCounter("sum", "baidu");
				System.out.println("from baidu:"+(double)baidu.getValue()/url.getValue());
			}
		} catch (ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
}
