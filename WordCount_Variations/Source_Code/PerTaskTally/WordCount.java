package assignment2;


import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;



public class WordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private IntWritable iwCount = new IntWritable();
	    
	    //  localMap store count for each map call 
    	private HashMap<String, Integer> localMap;
    	
    	public void setup(Context context) throws IOException, InterruptedException
    	{
    		localMap = new HashMap<String, Integer>();
    	}
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	       
	    	String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        String strWord;
	        char firstLetter;
	        while (tokenizer.hasMoreTokens()) 
	        {
	            word.set(tokenizer.nextToken());
	            strWord = word.toString();
	            firstLetter = strWord.charAt(0);
	            // Check if word is real
	            if (this.isReal(firstLetter))
	            {
	            	if (!localMap.containsKey(strWord))
	            	{
	            		localMap.put(strWord,1);
	            	}
	            	else
	            	{
	            		localMap.put(strWord, localMap.get(strWord) + 1);
	            	}
	            }
	        }
	    }
	    
	    // Checks if the Word is Real or not, by accepting its 1st Character
	    private boolean isReal(char aFirstCharOfWord)
	    {
	    	boolean result = 
	    			Character.toLowerCase(aFirstCharOfWord) == 'm' ||
            		Character.toLowerCase(aFirstCharOfWord) == 'n' ||
            		Character.toLowerCase(aFirstCharOfWord) == 'o' ||
            		Character.toLowerCase(aFirstCharOfWord) == 'p' ||
            		Character.toLowerCase(aFirstCharOfWord) == 'q';
	    	
	    	return result;
	    }
	    
	    // EMITTS to the Context
	    protected void cleanup (Context context) throws IOException, InterruptedException
	    {
	    	// EMIT to Context for each line in main text
	        for (String wrd : localMap.keySet())
	        {
	        	word.set(wrd);
	        	iwCount.set(localMap.get(wrd));
	        	
	        	context.write(word, iwCount);
	        }
	    }
	    
	 } 
	
    // Output types of Mapper should be same as arguments of Partitioner
    public static class customPartitioner extends Partitioner<Text, IntWritable> {
 
        @Override
        /* 
         * Returns m - OneOf['m', 'n', 'o', 'p', 'q']
         *  This determines the reduce partition that the word will go to
         *  Word starting from 'm' will goto partition 0
         *                     'n' will goto partition 1
         *                     'o' will goto partition 2
         *                     'p' will goto partition 3
         *                     'q' will goto partition 4
         *                     
         * */
        public int getPartition(Text key, IntWritable value, int numPartitions) {
 
            String myKey = key.toString().toLowerCase();
            char firstCharOfKey = myKey.charAt(0);
            return firstCharOfKey - 'm';
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
	        
	    Job job = new Job(conf, "wordcount");
	    
	    job.setJarByClass(WordCount.class);
	    // Setting the Partitioner
	    job.setPartitionerClass(customPartitioner.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    // Setting the Combiner here
	    //job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
	
}