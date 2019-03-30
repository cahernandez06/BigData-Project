package bAverageAlgo;

import java.io.IOException;
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

public class AverageAlgo {
	public static class MapProcess extends Mapper<LongWritable, Text, Text, Pair> {
		//private Logger logger = Logger.getLogger(MapProcess.class);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//logger.info("==== MAP PROCESS ====");
			String line = value.toString();
			String[] chunks = line.split(" ");
			String id = chunks[0];
			String lastChunk = chunks[chunks.length - 1];
			Integer number = 0;
			try {
				number = Integer.parseInt(lastChunk);
			} catch (NumberFormatException e) { }
			Pair p = null;
			p = new Pair(number, 1);
			context.write(new Text(id), p);
		}
	}

	public static class ReduceProcess extends Reducer<Text, Pair, Text, IntWritable> {
		//private Logger logger = Logger.getLogger(ReduceProcess.class);
		
		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
			//logger.info("==== REDUCE PROCESS ====");
			int sum = 0;
			int count = 0;
			for (Pair val : values) {
				sum += val.getKey();
				count += val.getValue();
			}
			context.write(key, new IntWritable(sum / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Logger logger = Logger.getLogger(ReduceProcess.class);
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(conf, "AverageAlgo");
		job.setJarByClass(AverageAlgo.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputValueClass(Pair.class);

		job.setMapperClass(MapProcess.class);
		job.setReducerClass(ReduceProcess.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		logger.info("==== AVERAGE NO IN-MAPPER COMBINER ====");

		job.waitForCompletion(true);
	}
}