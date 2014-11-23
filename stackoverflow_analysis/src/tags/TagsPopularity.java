package tags;


import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class TagsPopularity {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	    private Text count = new Text();
	    private Text word = new Text();
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] lineArr = line.split(",");
	        word.set("dummy");
	        count.set(lineArr[1]+"=="+lineArr[2]);
	        context.write(word, count);

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
	        
	 public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
		 
		 LinkedHashMap<String, Double> tagMap = new LinkedHashMap<String, Double>();
		 Double total = (double) 0;
		 double d = 7270322906.0;
		 Double den = new Double(d);
		 
		 ValueComparator bvc =  new ValueComparator(tagMap);
	     TreeMap<String,Double> sortedByVal_map = new TreeMap<String,Double>(bvc);
		 

	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException 
	    {
	    	
	    	 for (Text val : values)
	    	 {
	    		 String strVal = val.toString();
	    		 String[] strValArr = strVal.split("==");
	    		 String aTag = strValArr[0];
	    		 Double tagCount = Double.valueOf(strValArr[1]);
	    		 tagMap.put(aTag, tagCount);
	    		 total += tagCount;
	    	 }
	    	 
	    	 // System.out.println(total);
	    	 
	    	 for (Entry<String, Double> anEntry : tagMap.entrySet())
	    	 {
	    		 Double aVal = anEntry.getValue();
	    		 Double percent = (aVal/total)*100;
	    		 anEntry.setValue(percent);
	    	 }
	    	 
	    	 sortedByVal_map.putAll(tagMap);
	    	 System.out.println(sortedByVal_map.toString());
	    	
	        //context.write(key, new IntWritable(sum));
	    }
	    
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
	    
	    job.setJarByClass(TagsPopularity.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
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
