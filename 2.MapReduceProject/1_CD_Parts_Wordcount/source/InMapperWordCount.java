package aWordCount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class InMapperWordCount {

	public static class InMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Logger logger = Logger.getLogger(InMapper.class);
		private HashMap<Text, Integer> wordHashMap;

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			wordHashMap = new HashMap<Text, Integer>();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			//logger.info("Log information for wordcount");
			while (tokenizer.hasMoreTokens()) {
				Text word = new Text();
				word.set(tokenizer.nextToken().toLowerCase().replaceAll("[^A-Z,a-z]",""));
				//logger.info("Word " + word);
				if(!wordHashMap.containsKey(word)){
					wordHashMap.put(word, 1);
					//logger.info("Map Phase Hash: "+wordHashMap.get(word));
				}
				else{
					wordHashMap.put(word, wordHashMap.get(word)+1);
					//logger.info("Map Phase Hash: "+wordHashMap.get(word));
				}
			}    
		}
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			logger.info("---MAP CLOSE---");
			for (Entry<Text, Integer> entry : wordHashMap.entrySet()) {
				context.write(entry.getKey(), new IntWritable(entry.getValue()));
			}
		}
	}   
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		System.setProperty("hadoop.home.dir", "/");
		Job job = new Job(conf, "wordcount inMapper");
		
		FileSystem fs = FileSystem.get(conf);
		
	    if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.setJarByClass(InMapperWordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(InMapper.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}