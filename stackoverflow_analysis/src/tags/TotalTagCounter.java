package tags;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TotalTagCounter {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		 BufferedReader br = new BufferedReader(new FileReader("/home/vishrut/Desktop/exe/Tags/Tags.csv"));
		 long count = 0;
		 int counter = 0;
		    try {
		        
		        String line = br.readLine();
		        // skipping header
		        line = br.readLine();
		        
		        while (line != null) {
		            
		        	String[] data = line.split(","); 
		        	
		        	count = count + Long.parseLong(data[2]);
		        	counter++;
		            line = br.readLine();
		        }
		        
		    }catch(Exception e) {
		    	System.out.println(e.getMessage());
		    }finally {
		    	System.out.println("lines: "+ counter +" Total = " + count);
		        br.close();
		    }
	}

}
