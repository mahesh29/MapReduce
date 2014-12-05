package tags;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PopularityByTag {
	public static enum GlobalCounters {
		TOTAL_NUM_OF_TAGS // Stores the total Number of Tags
	}
	private static final long TAGS = 23585810; 

	public static class PopularityByTagMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text tag = null;
		private IntWritable count = null;
		private int total = 0;		

		private HashMap<String, Integer> tagCountMap;

		// Pattern for parsing the Tags-String from the Input Line
		Pattern tagPattern = Pattern.compile("<(.*)>");

		public void setup(Context context) throws IOException,
				InterruptedException {
			tagCountMap = new HashMap<String, Integer>();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();			
			Matcher mat = tagPattern.matcher(line);
			String tagString = "";

			while (mat.find()) {
				tagString = mat.group(1);
			}

			String[] tagStrArr = null;
			// Check if the Tags-String is present in the input line (not "")
			if (! tagString.isEmpty()) {
				tagStrArr = tagString.split("><");

				// Put in Array elements in Tags-Map
				for (String aTag : tagStrArr)
				{
					if (! tagCountMap.containsKey(aTag)) {
						tagCountMap.put(aTag, 1);
						total++;
					} else {
						tagCountMap.put(aTag, tagCountMap.get(aTag) + 1);
						total++;
					}
				}
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			
			for (String aTag : tagCountMap.keySet()) {
				tag = new Text(aTag);
				count = new IntWritable(tagCountMap.get(aTag));
				context.write(tag, count);
			}

			context.getCounter(GlobalCounters.TOTAL_NUM_OF_TAGS).increment(
					(long) total);
		}
	}

	public static class TagCustomPartitioner extends
			Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.hashCode() * 127) % numPartitions;
		}
	}

	public static class PopularityByTagReducer extends
			Reducer<Text, IntWritable, Text, DoubleWritable> {
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {			
			System.out.println(context.getCounter(GlobalCounters.TOTAL_NUM_OF_TAGS).getValue());
		}

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			long totalPerTag = 0;
			for (IntWritable countVal : values) {
				totalPerTag += countVal.get();
			}
			
			double popularityIndex = totalPerTag / (double)(TAGS) * 100;
			
			context.write(key, new DoubleWritable(roundTwoDecimals(popularityIndex)));
		}
		
		public double roundTwoDecimals(double d) {
		    DecimalFormat twoDForm = new DecimalFormat("#.####");
		    return Double.valueOf(twoDForm.format(d));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "TagsPopularity");

		job.setJarByClass(TagsPopularityFromPosts.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(PopularityByTagMapper.class);
		job.setReducerClass(PopularityByTagReducer.class);
		job.setPartitionerClass(TagCustomPartitioner.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
