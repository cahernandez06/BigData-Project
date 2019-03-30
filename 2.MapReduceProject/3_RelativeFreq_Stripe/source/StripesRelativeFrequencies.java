package cPart3;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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


public class StripesRelativeFrequencies {

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

		private Logger logger = Logger.getLogger(Map.class);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] window = line.split(" ");
			for(int i=0;i<window.length;i++){
				MapWritable stripeMap = new MapWritable();
				int j=i+1;
				for (; j < window.length; j++) {
					if(!window[i].equals(window[j])){
						if(!stripeMap.containsKey(window[j])){
							stripeMap.put(new Text(window[j]), new FloatWritable(1));
						}
						else{
							stripeMap.put(new Text(window[j]), new FloatWritable((((FloatWritable) stripeMap.get(window[j])).get())+1));
						}
					}else break;
				}
				//logger.info("MAPPER OUTPUT: " + window[i] + " ( "+ stripeMap.values()+"," + stripeMap.keySet() +" )");
				context.write(new Text(window[i]), stripeMap);
			}
		}
	} 

	public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
		private Logger logger = Logger.getLogger(Reduce.class);
		public void reduce(Text key, Iterable<MapWritable> values, Context context) 
				throws IOException, InterruptedException {
			float total = 0;
			MapWritable finalStripeMap = new MapWritable();
			for (MapWritable val : values) {
				for (Entry<Writable, Writable> mapWritable : val.entrySet()) {
					if(!finalStripeMap.containsKey(mapWritable.getKey())){
						finalStripeMap.put(mapWritable.getKey(), mapWritable.getValue());
						total=((FloatWritable) mapWritable.getValue()).get();
					}
					else{
						float value=((FloatWritable) finalStripeMap.get(mapWritable.getKey()))
								.get()
								+ ((FloatWritable) mapWritable.getValue()).get();
						finalStripeMap.put(mapWritable.getKey(), new FloatWritable(value));
						total+= ((FloatWritable) finalStripeMap.get(mapWritable.getKey())).get();
					}
				}
			}
			float sum = 0;
			for (Entry<Writable, Writable> entry : finalStripeMap.entrySet()) {
				sum += ((FloatWritable) entry.getValue()).get();
			}
			StringBuilder stb = new StringBuilder();
			for (Entry<Writable, Writable> entry : finalStripeMap.entrySet()) {
				float value = ((FloatWritable) entry.getValue()).get() / sum;
				finalStripeMap.put(entry.getKey(), new FloatWritable(value));
				stb.append("[").append(entry.getKey()).append(" = ").append(entry.getValue()).append("] ");
			}
			//logger.info("REDUCER OUTPUT: "+stb.toString().trim());
			context.write(key, new Text(stb.toString().trim()));
		}
		
	}

	public static void main(String[] args) throws Exception {

		Logger logger = Logger.getLogger(Reduce.class);
		long startTime = System.currentTimeMillis();

		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();

		Job job = new Job(conf, "StripesRelFreq");

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);

		job.setJarByClass(StripesRelativeFrequencies.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

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
