package LastProject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class JumpRate {
	
	public static class Map extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Pattern r = Pattern.compile("(\\d+.\\d+.\\d+.\\d+).*\\[((.*?):.*)\\] \"(.*)\"");
			Matcher m = r.matcher(value.toString());
			if (m.find()) {
				String s = m.group(4).split(" ")[1];
				if(s.equals("/")){
					context.write(new Text(m.group(1)),new Text(m.group(2)+"\troot"));
				}else if(s.contains("show")){
					context.write(new Text(m.group(1)),new Text(m.group(2)+"\tshow"));
				}else if(s.contains("musicians")){
					context.write(new Text(m.group(1)),new Text(m.group(2)+"\tmusicians"));
				}else {
					context.write(new Text(m.group(1)),new Text(m.group(2)+"\telse"));
				}
			} 
		}
		
	}
/*	public static class Partition extends Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int num) {
			// TODO Auto-generated method stub
			if(key.toString().length()>10){
				return 0;
			}else{
				return 1;
			}
		}
		
	}*/
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		int count=0;
		HashMap<String , Integer> res = new HashMap<String , Integer>();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			TreeMap<String , String> map = new TreeMap<String , String>();
			for (Text text : values) {
				String time = text.toString().split("\t")[0];
				String target = text.toString().split("\t")[1];
				map.put(time, target);
				count++;
			}
			Iterator<Entry<String,String>> it = map.entrySet().iterator();
			String lastTarget=it.next().getValue(); 
			while(it.hasNext()){
				String target=lastTarget+"-"+it.next().getValue();
				try {
					res.put(target, res.get(target) + 1);
				} catch (Exception e) {
					res.put(target, 1);
				}
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			for (Entry<String,Integer> entry : res.entrySet()) {
				context.write(new Text(entry.getKey()), new Text(String.format("%.2f",(double)entry.getValue()*100/count)+"%"));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "IpCount");
		job.setJarByClass(JumpRate.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}