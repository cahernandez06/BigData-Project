package bAverageCombAlgo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

public class AverageCombAlgo {
	public static class MapProcess extends Mapper<LongWritable, Text, Text, Pair> {
		private Logger logger = Logger.getLogger(MapProcess.class);
		private Map<String, Pair> H;

		@Override
		protected void setup( Mapper<LongWritable, Text, Text, Pair>.Context context) throws IOException, InterruptedException {
			logger.info("==== IN MAPPER COMBINER SETUP ====");
			H = new HashMap<String, Pair>();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//logger.info("==== IN MAPPER COMBINER MAP ====");
			String line = value.toString();
			String[] chunks = line.split(" ");
			String id = chunks[0];
			String lastChunk = chunks[chunks.length - 1];
			Integer number = 0;
			try {
				number = Integer.parseInt(lastChunk);
			} catch (NumberFormatException e) { }
			Pair p = null;
			if (!H.containsKey(id)) {
				p = new Pair(number, 1);
			} else {
				p = H.get(id);
				p.setKey(p.getKey() + number);
				p.setValue(p.getValue() + 1);
			}
			H.put(id, p);
		}

		@Override
		protected void cleanup(	Mapper<LongWritable, Text, Text, Pair>.Context context)	throws IOException, InterruptedException {
			//logger.info("==== IN MAPPER COMBINER CLEANUP ====");
			for (Entry<String, Pair> entry : H.entrySet()) {
				context.write(new Text(entry.getKey()), entry.getValue());
			}
		}
	}

	public static class ReduceProcess extends Reducer<Text, Pair, Text, IntWritable> {
		private Logger logger = Logger.getLogger(ReduceProcess.class);
		public void reduce(Text key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {
			//logger.info("==== REDUCER ====");
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
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(conf, "AverageInMapCombAlgo");
		job.setJarByClass(AverageCombAlgo.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputValueClass(Pair.class);

		job.setMapperClass(MapProcess.class);
		job.setReducerClass(ReduceProcess.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}