package Assignment3;


import java.io.IOException;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class FlightDataCompChallenge {

	public enum GlobalCounters 
	{
		SUM_TOTAL_OF_DELAYS, // Stores the 'Summation' of all Flight Delays 
		NUMBER_OF_DELAYS     // Stores the 'Total number' of Flight Delays
	}
	
	public static class FlightDataMapper extends Mapper<Object, Text, Text, Text> 
	{
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			// Split input by comma & store in array 
			String[] anEntry = value.toString().split(",");
			
			// Extract required values from anEntry
			String 
			    flightYear = anEntry[0],
			    flightMonth = anEntry[2],
			    flightDay = anEntry[3],
				flightDate = anEntry[5], 
				originCityCode = anEntry[11].replace("\"", ""), 
				destCityCode = anEntry[18].replace("\"", ""),
				departureTime = anEntry[26],
				arrivalTime = anEntry[37],
				delay = anEntry[39],
				cancelled = anEntry[43],
				diverted = anEntry[45];

			boolean isWithinDateRange = false;
			try 
			{
				String sArr[]=flightDate.split("-");
				Date newDate=new Date(sArr[0] + "/" + sArr[1] + "/" + sArr[2]);
				/*  
				 *  Checking if the 'flightDate' lies 
				 *  between end of may'07 and start of june'08.
				*/
				isWithinDateRange = newDate.after(new Date("2007/05/31")) 
									&& newDate.before(new Date("2008/06/01"));
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
			
			/* LEG-1
			 * Checking whether flight Leaves ORD, but does NOT land at JFK
			 * */
			Boolean isValidLEG_1 = (originCityCode.equals("ORD") 
								  && !destCityCode.equals("JFK"));
			
			/* LEG-2
			 * Checking whether flight lands at JFK, but NOT originated from ORD
			 * */
			Boolean isValidLEG_2 = (!originCityCode.equals("ORD") 
									&& destCityCode.equals("JFK"));
			
			/* 
			 * Checking whether flight is NOT Cancelled and NOT Delayed
			 * */
			if ((isValidLEG_1 || isValidLEG_2) 
					&& isWithinDateRange 
					&& cancelled.equals("0.00") 
					&& diverted.equals("0.00")) 
			{
				/*
				 * aKey would be one of :
				 * 
				 *    -- 'destCityCode - flightDate'
				 *    -- 'originCityCode - flightDate'
				 *    
				 * .. with the character '-' being a delimiter  
				 *  
				 * */
				Text aKey;

				if (originCityCode.equals("ORD"))
					// -- 'destCityCode - flightDate'
					aKey = new Text(destCityCode + "-" + flightDate);
				else
					// -- 'originCityCode | flightDate'
					aKey = new Text(originCityCode + "-" + flightDate);
				
				// Value ::  originCityCode - departureTime - arrivalTime - delay
				//  .. with the character '-' being a delimiter 
				String valString = originCityCode + "-" + departureTime
						                          + "-" + arrivalTime 
						                          + "-" + delay;
				Text val = new Text(valString);
				context.write(aKey, val);
			}
		}
	}

	public static class DelayCalcReducer extends Reducer<Text, Text, Text, FloatWritable> 
	{
		private float fResult;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			// list_ORD    : stores flights originating from ORD to terminal-X
			ArrayList<String> list_ORD = new ArrayList<String>();
			
			// list_Others : stores all others
			ArrayList<String> list_Others = new ArrayList<String>();
			
			for (Text aVal : values) 
			{
				String strVal = aVal.toString();
				String[] arrValTokens = strVal.split("-");
				
				if (arrValTokens[0].equals("ORD")) 
				{
					list_ORD.add(strVal);
				} 
				else 
				{
					list_Others.add(strVal);
				}
			}
			
			/* Check for flight arrival time to terminal-X
			 * to be lesser than departure time to JFK
			 * */
			String[] entry_ORD, entry_Others;
			
			for (int i = 0; i < list_ORD.size(); i++) 
			{
				entry_ORD = list_ORD.get(i).split("-");
				
				for (int j = 0; j < list_Others.size(); j++) 
				{
					entry_Others = list_Others.get(j).split("-");
					// Check if time is valid
					if (isValidTime(entry_ORD, entry_Others)) 
					{
						fResult = Float.parseFloat(entry_ORD[3])
								+ Float.parseFloat(entry_Others[3]);

						// Increment Global counters
					    context.getCounter(GlobalCounters.SUM_TOTAL_OF_DELAYS).increment((long) fResult);
						context.getCounter(GlobalCounters.NUMBER_OF_DELAYS).increment(1);
					}
				}
			}
		}

		private boolean isValidTime(String[] aEntry_ORD, String[] aEntry_Others) 
		{
			
			boolean result = !(aEntry_ORD[2].trim().length() == 0 // arrival/departure Time not Empty
					            || aEntry_Others[1].trim().length() == 0)
					            /* Arrival Time, less than Departure Time*/
			               && (Integer.parseInt(aEntry_ORD[2].replace("\"", "")) 
			                    < Integer.parseInt(aEntry_Others[1].replace("\"", "")));
			return result;
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: FlightDataCompChallenge <input> <output>");
			System.exit(2);
		}
		Job job = new Job(conf, "Average Flight Delay Calculator");
		job.setJarByClass(FlightDataCompChallenge.class);
		job.setMapperClass(FlightDataMapper.class);
		// job.setCombinerClass(DelayCalcReducer.class);
		job.setReducerClass(DelayCalcReducer.class);
		/* 10 Reduce Tasks*/
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(true)) 
		{
			// Get all the Global counters
			Counters globalCounters = job.getCounters();
			
			double SumTotalOfDelays = (double)globalCounters
												.findCounter(GlobalCounters.SUM_TOTAL_OF_DELAYS)
												.getValue();
			double NumberOfDelays = (double)globalCounters
												.findCounter(GlobalCounters.NUMBER_OF_DELAYS)
												.getValue();
			// Console out the result
			System.out.println("Average Flight Delay = " + SumTotalOfDelays/NumberOfDelays);
			System.exit(0);
		}
		System.exit(1);
	}
}
