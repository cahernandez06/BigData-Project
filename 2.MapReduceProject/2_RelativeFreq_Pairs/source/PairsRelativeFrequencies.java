package cPart2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class PairsRelativeFrequencies {

	public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Logger logger = Logger.getLogger(Map.class);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] window = line.split(" ");
			for(int i=0;i<window.length;i++){
				int j=i+1;
				for (; j < window.length; j++) {
					if(!window[i].equals(window[j])){
						context.write(new Pair(window[i],window[j]), one);
						context.write(new Pair(window[i],"*"), one);
						//logger.info("MAPPER OUTPUT: (" + window[i] + "," + window[j] +" ), "+ one);
						//logger.info("MAPPER OUTPUT: (" + window[i] + ", * ), "+ one);
					}else break;
				}
			}
		}
	} 

	public static class Reduce extends Reducer<Pair, IntWritable, Pair, FloatWritable> {
		public final static String ASTERISK="*";
		private float total;
		private Logger logger = Logger.getLogger(Reduce.class);
		@Override
		protected void setup(
				Reducer<Pair, IntWritable, Pair, FloatWritable>.Context context)throws IOException, InterruptedException {
			total = 0;
		}

		public void reduce(Pair key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum=0;
			for (IntWritable val : values) {
				sum+=val.get();
			}
			if(key.getValue().equals(ASTERISK)){
				total=sum;
			}
			else{
				//logger.info("REDUCER OUTPUT: (("+ key.getKey() + "," + key.getValue() +"), ["+sum/total+"])");
				context.write(new Pair(key.getKey(),key.getValue()),new FloatWritable(sum/total));
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		
		Logger logger = Logger.getLogger(Reduce.class);
		long startTime = System.currentTimeMillis();

		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PairsRelFreq");
		
		//Automatic removal of “output” directory before job execution
				FileSystem fs = FileSystem.get(conf);
				fs.delete(new Path(args[1]), true);

		job.setJarByClass(PairsRelativeFrequencies.class);

		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputKeyClass(Pair.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	    long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    logger.info("TOTAL TIME: " + elapsedTime);
	}
}
