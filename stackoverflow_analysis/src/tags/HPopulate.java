package tags;


import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HPopulate {
	
		// Custom Schema Element Names
		public static final String TABLE_NAME 	= "Post_Table";
//		public static final String POST_ID 		= "Post_ID";
		public static final String COL_FAMILY 	= "Post_Family";
		public static final String HASHES 		= "Hashes";
		
		// Schema Element Name definitions to Bytes
//		public static final byte[] BYTES_POST_ID 	= Bytes.toBytes(POST_ID);
		public static final byte[] BYTES_COL_FAMILY = Bytes.toBytes(COL_FAMILY);
		public static final byte[] BYTES_HASHES 	= Bytes.toBytes(HASHES);
		
		
		static class HBaseFlightDataMapper extends Mapper <Object, Text, ImmutableBytesWritable, Writable>
		{
			// Pattern for parsing the Tags-String from the Input Line
	    	Pattern tagPattern = Pattern.compile("<(.*)>");
	    	
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
			{
				// Split input by comma & store in array 
				String valueToString = value.toString();
				String[] anEntry = valueToString.split(",");
				
				// Extract required fields from the line
				String postID = anEntry[0];
				int postTypeID = Integer.parseInt(anEntry[1]);

		        
		        String tagStrArr = anEntry[10];
			
		        boolean isPostAQuestion = (postTypeID == 1);
				
				if (isPostAQuestion)
				{
					Put hBaseRow = new Put(Bytes.toBytes(postID));
					hBaseRow.add(BYTES_COL_FAMILY, BYTES_HASHES, Bytes.toBytes(tagStrArr));
					
					ImmutableBytesWritable ibwCompKey = new ImmutableBytesWritable(Bytes.toBytes(postID));
					// Emit from Map
					context.write(ibwCompKey, hBaseRow);
				}
			
			}
		}
		
		public static void main(String[] args) throws Exception
		{
				
				Configuration conf = HBaseConfiguration.create();
			    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			    if (otherArgs.length != 1) 
			    {
			      System.err.println("Usage: Hpopulate <in>");
			      System.exit(2);
			    }
			    
				HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
			    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME); 
			    htd.addFamily(new HColumnDescriptor(COL_FAMILY));
			    
			    HBaseAdmin hba = new HBaseAdmin(hconfig);
			    if(hba.tableExists(TABLE_NAME))
			    {
			    	hba.disableTable(TABLE_NAME);
			    	hba.deleteTable(TABLE_NAME);
			    }
			    hba.createTable(htd);
			    hba.close();
			    
			    Job job = new Job(conf, "HBase Populte");
			    job.setJarByClass(HPopulate.class);
			    job.setMapperClass(HBaseFlightDataMapper.class);
			    job.setOutputFormatClass(TableOutputFormat.class);
			    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
			    job.setNumReduceTasks(0);
			    job.setOutputKeyClass(ImmutableBytesWritable.class);
			    job.setOutputValueClass(Put.class);
			    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			    if (job.waitForCompletion(true)) 
					System.exit(0); 
				else 
					System.exit(1);
			}


}
