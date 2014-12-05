package tags;


import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class TagsPopularityFromPosts {
	
//	public enum GlobalCounters 
//	{
//		TOTAL_NUM_OF_TAGS // Stores the total Number of Tags 
//	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		static enum GlobalCounters 
		{
			TOTAL_NUM_OF_TAGS // Stores the total Number of Tags 
		}
		
	    private Text tagKey = new Text();
	    private IntWritable countVal = new IntWritable();
	    // Record total number of Tags per map task
	    private int totalVal=0;
	    	    
	    //  localMap store count for each map call 
    	private HashMap<String, Integer> perTaskTagsMap;
    	
    	// Pattern for parsing the Tags-String from the Input Line
    	Pattern tagPattern = Pattern.compile("<(.*)>");
    	
    	/*
    	 * SETUP : Initialize the Hashmap for In-mapper Combining Per MapTask
    	 * */
    	public void setup(Context context) throws IOException, InterruptedException
    	{
    		perTaskTagsMap = new HashMap<String, Integer>();
    	}
	    
    	/*
    	 * Map Function
    	 * */
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	    {
	        String line = value.toString();
	        // Mactch the pattern to each line
	        Matcher mat = tagPattern.matcher(line);
	        String tagStr = "";
	        
	        while (mat.find())
	        {
	            tagStr = mat.group(1);
	        }
	        
	        String[] tagStrArr = null;
	        // Check if the Tags-String is present in the input line (not "")
	        if (tagStr != "")
	        {
	        	tagStrArr = tagStr.split("><");

	        	// Put in Array elements in Tags-Map
	        	for (String aTag : tagStrArr)
	        	if (!perTaskTagsMap.containsKey(aTag))
            	{
	        		perTaskTagsMap.put(aTag,1);
	        		totalVal += 1;
            	}
            	else
            	{
            		perTaskTagsMap.put(aTag, perTaskTagsMap.get(aTag) + 1);
            		totalVal += perTaskTagsMap.get(aTag) + 1;
            	}
	        }
	    }
	    
	    
	    /*
	     * CLEANUP : Emit out key values
	     * */
	    protected void cleanup (Context context) throws IOException, InterruptedException
	    {
	    	// EMIT to Context for each line in main text
	        for (String aTag : perTaskTagsMap.keySet())
	        {
	        	tagKey.set(aTag);
	        	countVal.set(perTaskTagsMap.get(aTag));
	        	
	        	context.write(tagKey, countVal);
	        }
	        
	        // Emit a DUMMY key with total Tag value for each map task
//	        tagKey.set("dummyKey");
//	        countVal.set(totalVal);
//	        context.write(tagKey, countVal); 
	        context.getCounter(GlobalCounters.TOTAL_NUM_OF_TAGS).increment((long)totalVal);
	    }
	 } 
	
	        
	
	public static class TagCustomPartitioner extends
	Partitioner<Text, IntWritable> {
			/**
			 * Based on the configured number of reducer, this will partition the
			 * data approximately evenly based on number of unique post Ids
			 */
			@Override
			public int getPartition(Text key, IntWritable value,
					int numPartitions) {
				// multiply by 127 to perform some mixing
				return Math.abs(key.hashCode() * 127) % numPartitions;
			}
			}
	
	
	 public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> 
	 { 

		 private long mapperCounter;
		 @Override
		 public void setup(Context context) throws IOException, InterruptedException
		 {
	        Configuration conf = context.getConfiguration();
	        Cluster cluster = new Cluster(conf);
	        Job currentJob = cluster.getJob(context.getJobID());
	        mapperCounter = context.getCounter(Map.GlobalCounters.TOTAL_NUM_OF_TAGS).getValue();  
	        System.out.println(mapperCounter);
	     }

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException 
	    {
	    	 String aTagFromKey = key.toString();
	    	 Double intDelayVal =0.0;
	    	 for (IntWritable countVal : values)
				{   
				    intDelayVal += (double) countVal.get();
				    
				}
	    	 double popularity = intDelayVal / context.getCounter(Map.GlobalCounters.TOTAL_NUM_OF_TAGS).getValue();
	    	 System.out.println(key.toString()+" :: "+popularity);
	    	 
	    }
	 }
	        
	 public static void main(String[] args) throws Exception 
	 {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "TagsPopularity");
	    
	    job.setJarByClass(TagsPopularityFromPosts.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setPartitionerClass(TagCustomPartitioner.class);
	    //job.setNumReduceTasks(1);
//		job.setGroupingComparatorClass(CustomGroupComparator.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
	
}
