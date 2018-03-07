package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;

public class addedFunctions {
	
	/**
	 * Get the all the combinations from an ArrayList - right now only with two elements
	 * 
	 * @parameter ArrayList list
	 * @return ArrayList combo
	 */
	public static ArrayList<String> getCombinations(ArrayList<String> list) {
		
		// Final ArrayList which would be return
		ArrayList<String> combo = new ArrayList<String>();
				
		// gets the size of the list
		int size = list.size();
				
		// customized Hashmap to store multiple keys vlaues 
		HashMap<String,List<String>> map = new HashMap<String,List<String>>();
				
		for ( int i = 0; i < size; i++ )
		{
			//create list for values - declearing it in here so that everytime it removes the values	
			List<String> hashValues = new ArrayList<String>();
					
			for ( int j = i + 1; j < size; j++ )
			{
						
				// add values to "hasValues"
				hashValues.add(new String(list.get(j)));
						
			}
				
			// add list with the value of i as key in the HashMap
			map.put(new String(list.get(i)), hashValues);
					
		}
				
		// Set to map the hashmap to insert the values in it
		Set<Map.Entry<String,List<String>>> st = map.entrySet();

		for ( Map.Entry<String,List<String>> me : st ) {
					
			// Empty string
			String val = "";
			
			// getting the key for element "me"
			val += me.getKey().toString();
					
			// List of key "val"
			List<String> listVal = me.getValue();
					
			for (String singleListVal : listVal)
			{
				// Empty string
				String singleCombination = "";
				singleCombination += val;
				singleCombination += " ";
				singleCombination += singleListVal;
						
				// Adding the combination to the ArrayList
				combo.add(singleCombination);

			}

		}

		// Returning the arrayList
		return combo;

	} // end of getCombinations()
	
	/*
	 * Determines if an item with the specified frequency has minimum support or not.
	 */
	public static boolean hasMinSupport ( double minSupPercent, int maxNumTxns, long itemCount )
	{
		boolean hasMinSupport = false;
		long minSupport = (long)((double)(minSupPercent * maxNumTxns))/100;
		if ( itemCount >= minSupport ) {
			hasMinSupport = true;
		}

		return hasMinSupport;
	}
	
	
	/**
	 * Read everything from a folder by reading all files in the folder
	 * @param conf
	 * @param filePath
	 */
	public static void listAllFilesFromFolder( Configuration conf, String filePath) {
		
		try{
        	
        	System.out.println("In the try main");
        	
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(new Path(filePath));  // you need to pass in your hdfs path

            System.out.println("status: " + status);
            
            for ( int i = 0; i < status.length; i++ ) {
            	
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line = br.readLine();
                
                while (line != null){
                    System.out.println(line);
                    line=br.readLine();
                }
            }
        }catch(Exception e){
            System.out.println("File not found");
        }
		
	}
	
	/**
	 * Function that will merge the output partition files and then save as a single file
	 * @param conf
	 * @param source
	 * @param dest
	 * @return true if copyMerge successful, false otherwise
	 */
	public static boolean copyMergeFiles( Configuration conf, String source, String dest) {
		
//		System.out.println("In copyMerge");
		
		try {
			
//			System.out.println("In Try");
			
//			System.out.println(source);
//			System.out.println(dest);
			
			// Default fileSystem
			FileSystem fs = FileSystem.get(conf);
			
//			System.out.println("fs: " + fs);
			
			// Input Folder Path
			Path inputPath = new Path(source);
			
//			System.out.println("inputPath: " + inputPath);
			
			// Output Folder Path
			Path finalOutputPath = new Path(dest);
			
//			System.out.println("outputPath: " + finalOutputPath);
			
			// Use copy merge to combine all of the input files
			return FileUtil.copyMerge(fs, inputPath, fs, finalOutputPath, false, conf, null);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Default return
		return false;
		
	}
	
	/**
	 * Delete the given file from the distributed file system.
	 */
	public static void deleteOutputFolder(String outputPath, Configuration conf) throws IOException {
//		FileSystem fs = FileSystem.get(conf);
//		fs.delete(output, true);
		
		// Output path
        Path output = new Path(outputPath);
        FileSystem hdfs = FileSystem.get(conf);

        // delete existing directory
        if (hdfs.exists(output)) {
            
        	if ( hdfs.delete(output, true) ) 
        		System.out.println("Output folder DELETED: " + outputPath);
        	else 
        		System.out.println("Output folder not deleted");
            
        }
	}
	
	/**
	 * 
	 * @param list
	 * @return results
	 */
	public static ArrayList<String> getCombinations1(ArrayList<String> list) {
	
		ArrayList<String> results= new ArrayList<String>();
		
		String firstElement, secondElement;

		for(int i = 0; i < list.size() - 1; i++)
		    for (int j = i+1; j < list.size(); j++)  {
		    	
		    	// getting two elements
		    	firstElement = list.get(i);
		    	secondElement = list.get(j);
		    	
		    	// checking palindrome
		    	if ( Integer.parseInt(firstElement) > Integer.parseInt(secondElement) )
		    		results.add(secondElement + " " + firstElement);
		    	else
		        results.add(firstElement + " " + secondElement);
		    }
		
		return results;
	}
	
	/**
	 * Return the support for a number 
	 * 
	 * @param	number	number for that we need to get the support
	 * @param	maxTrnx	maximum number of transactions
	 * 
	 * @return	return the calculated support for a given number
	 */
	public static float getSupport( float number, Integer maxTrnx ) {
		
		return ( number / (float) maxTrnx );
		
	}
	
	
		
	

}
