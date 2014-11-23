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

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
	    private Text tagKey = new Text();
	    private IntWritable countVal = new IntWritable();
	    private int totalVal=0;
	    	    
	    //  localMap store count for each map call 
    	private HashMap<String, Integer> perTaskTagsMap;
    	
    	// Pattern for parsing the Tags-String from the Input Line
    	Pattern tagPattern = Pattern.compile("<(.*)>");
    	
    	public void setup(Context context) throws IOException, InterruptedException
    	{
    		perTaskTagsMap = new HashMap<String, Integer>();
    	}
	    
    	
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	    {
	        String line = value.toString();
	        
	        Matcher mat = tagPattern.matcher(line);
	        String tagStr = "";
	        
	        while (mat.find())
	        {
	            tagStr = mat.group(1);
	        }
	        
	        String[] tagStrArr = null;
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
	    
	    
	    // EMITTS to the Context
	    protected void cleanup (Context context) throws IOException, InterruptedException
	    {
	    	// EMIT to Context for each line in main text
	        for (String aTag : perTaskTagsMap.keySet())
	        {
	        	tagKey.set(aTag);
	        	countVal.set(perTaskTagsMap.get(aTag));
	        	
	        	context.write(tagKey, countVal);
	        }
	        tagKey.set("dummyKey");
	        countVal.set(totalVal);
	        context.write(tagKey, countVal);       
	    }
	 } 
	
	public static class CustomGroupComparator extends WritableComparator
	{
		protected CustomGroupComparator()
		{
			super(Text.class, true);
		}
		@Override
		public int compare(WritableComparable comp1, WritableComparable comp2)
		{
			Text compKey1 =  (Text)comp1;
			Text compKey2 =  (Text)comp2;
			
			return compKey1.compareTo(compKey2);
		}
	}
	        
	 public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> 
	 { 
		 LinkedHashMap<String, Double> tagMap;
		 Double total = (double) 0;
		 
		 ValueComparator bvc;
	     TreeMap<String,Double> sortedByVal_map;
		 
	     
	     public void setup(Context context) throws IOException, InterruptedException
    	{
	    	 tagMap = new LinkedHashMap<String, Double>();
	    	 bvc =  new ValueComparator(tagMap);
	    	 sortedByVal_map = new TreeMap<String,Double>(bvc);
    	}
	     

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException 
	    {
	    	 String aTagFromKey = key.toString();
	    	 for (IntWritable countVal : values)
				{   
				    Double intDelayVal = (double) countVal.get();
				    
					if(tagMap.containsKey(aTagFromKey))
					{
						tagMap.put(aTagFromKey, tagMap.get(aTagFromKey) + intDelayVal);
					
					}
					else
					{
						tagMap.put(aTagFromKey, intDelayVal);
					}
				}
	    }
	    
	    // EMITTS to the Context
	    protected void cleanup (Context context) throws IOException, InterruptedException
	    {
	    	// EMIT to Context for each line in main text
	    	
	    	Double numOfTotalTags = tagMap.remove("dummyKey");
	    	
	    	 for (Entry<String, Double> anEntry : tagMap.entrySet())
	    	 {
	    		 Double aVal = anEntry.getValue();
	    		 Double percent = (aVal/numOfTotalTags)*100;
	    		 anEntry.setValue(percent);
	    	 }
	    	 
	    	 sortedByVal_map.putAll(tagMap);
	    	 
	    	 
	    	 for (Entry<String, Double> anEntry : sortedByVal_map.entrySet())
	    	 {
	    		 Text opKey = new Text(anEntry.getKey());
	    		 DoubleWritable opVal = new DoubleWritable(anEntry.getValue());
	    		 context.write(opKey, opVal);
	    	 }
  
	    }
	    
	    // Comparator for sorting the Hashmap by value
	    class ValueComparator implements Comparator<String> {

	        LinkedHashMap<String, Double> base;
	        public ValueComparator(LinkedHashMap<String, Double> base) {
	            this.base = base;
	        }

	        // Note: this comparator imposes orderings that are inconsistent with equals.    
	        public int compare(String a, String b) {
	            if (base.get(a) >= base.get(b)) {
	                return -1;
	            } else {
	                return 1;
	            } // returning 0 would merge keys
	        }
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
	    
	    job.setNumReduceTasks(1);
		job.setGroupingComparatorClass(CustomGroupComparator.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
	
}
