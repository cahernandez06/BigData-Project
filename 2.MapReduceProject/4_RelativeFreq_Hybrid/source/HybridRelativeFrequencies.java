package dPart4;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import cPart3.StripesRelativeFrequencies.Reduce;

public class HybridRelativeFrequencies {

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
						//logger.info("MAPPER OUTPUT: (" + window[i] + "," + window[j] +"), "+ one);
					}else break;
				}
			}
			
		}
	} 

	public static class Reduce extends Reducer<Pair, IntWritable, Text, Text> {
		MapWritable finalStripeMap = new MapWritable();
		String prev=null;
		int sum=0;
		private Logger logger = Logger.getLogger(Reduce.class);
		public void reduce(Pair key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			if(!key.getKey().equals(prev) && prev!=null){
				emit(context);
			}
			float total = 0;
			for (IntWritable val : values) {
				total += val.get();
			}
			finalStripeMap.put(new Text(key.getValue()), new FloatWritable(total));
			prev = key.getKey();

		}

		@Override
		protected void cleanup(Reducer<Pair, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			emit(context);
		}

		private void emit(Context context) throws IOException, InterruptedException{
			StringBuilder stb = new StringBuilder();
			float sum = 0;
			for (Entry<Writable, Writable> entry : finalStripeMap.entrySet()) {
				sum += ((FloatWritable)entry.getValue()).get();
			}

			for (Entry<Writable, Writable> entry : finalStripeMap.entrySet()) {
				float freq = ((FloatWritable)entry.getValue()).get() / sum;
				finalStripeMap.put(entry.getKey(), new FloatWritable(freq));
				stb.append("[").append(entry.getKey()).append(" = ").append(entry.getValue()).append("] ");
			}
			//logger.info("REDUCER OUTPUT: "+stb.toString().trim());
			context.write(new Text(prev), new Text(stb.toString().trim()));
			finalStripeMap = new MapWritable();
		}
	}

	public static void main(String[] args) throws Exception {

		Logger logger = Logger.getLogger(Reduce.class);
		long startTime = System.currentTimeMillis();

		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();

		Job job = new Job(conf, "HybridRelFreq");

		//Automatic removal of “output” directory before job execution
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);

		job.setJarByClass(HybridRelativeFrequencies.class);

		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(IntWritable.class);

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
