package apriori;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.HashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

//import org.apache.hadoop.mapreduce.Mapper.Context;


import utils.addedFunctions;

public class aprioriMapper2 extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	// HashMap for readFile
//	private HashMap<String, Integer> combWithValues = new HashMap<String, Integer>();
	
	// ArrayList for readFile
	ArrayList<String> oneItemSetKeys = new ArrayList<String>();
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		
	    	try{
	//    		System.out.println("In the try in setup");
	    		
	    		Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	    		
	//    		System.out.println("Setup in Map 2");
	//    		System.out.println(stopWordsFiles);
	    		
	    		if(stopWordsFiles != null && stopWordsFiles.length > 0) {
	    			for(Path stopWordFile : stopWordsFiles) {
	//    				System.out.println(stopWordFile);
	    				readFile(stopWordFile, context.getConfiguration());
	    			}
	    		}
	    		
//	    		System.out.println("printing c");
//	            
//            for ( String asd: oneItemSetKeys )
//            		System.out.println("[" + asd + "]");
	    		
	    	} catch(IOException ex) {
	    		System.err.println("Exception in mapper setup: " + ex.getMessage());
	    	}
    }
	
	@Override
    protected void map(LongWritable offset, Text input, Context context) throws IOException, InterruptedException {
		
		// Getting input from file 
		String textToString = input.toString();
		
		// Trimming tail "," from end
		String[] mainWords = textToString.split(",");
		
//		System.out.println("MainWords: " + mainWords[0]);

		// Splitting mainWords with space
        String[] words = mainWords[0].split("\\s");
        
        // Initial ArrayList - now contains one transaction
     	ArrayList<String> list = new ArrayList<String>();

        for ( int i = 0; i < words.length; i++ ) {
        	
	        	// adding all 1-itemset to the the list
	        	list.add(words[i]);
	//        	list.add(words[i].trim().substring(0, words[i].length()-1));
	
	        	// 1-itemSet
	//            context.write(new Text(words[i]), new IntWritable(1));

        }
        
        /**
         * Start - New part of code
         */
        
//        System.out.println("printing list");
//
//        for ( String asd: list )
//        	System.out.println("[" + asd + "]");
        
        // ArrayList to hold the final combinations
		ArrayList<String> getListFromComb = new ArrayList<String>();
        
        // Get the combinations with oneItemSetKeys that is found from readFile function
		getListFromComb = addedFunctions.getCombinations1(oneItemSetKeys);
		
//		System.out.println("printing getListFromComb");
        
        for ( String ATwoItemSet: getListFromComb ) {
//        		System.out.println(ATwoItemSet);
        	
	        	 String separateWords[] = ATwoItemSet.split("\\s");
	        	 
	//        	 System.out.println("contains in list");
	        	 
	        	 // Getting the items from the combinations
	        	 // Trimming for precausion + removing last trailing ","
//	        	 String firstItem = separateWords[0].trim().substring(0, separateWords[0].length()-1);
//	        	 String secondItem = separateWords[1].trim().substring(0, separateWords[1].length()-1);
	        	 
	        	 String firstItem = separateWords[0];
	        	 String secondItem = separateWords[1];
	        	 
//	        	 System.out.println("[" + firstItem + "]");
//	        	 System.out.println("[" + secondItem + "]");
//	        	 System.out.println(list.contains(firstItem));
//	        	 System.out.println(list.contains(secondItem));
	        	
	        	 // Checking if this combination is available in the main dataset or not
	        	 if ( list.contains(firstItem) && list.contains(secondItem) ) {
	//        		 System.out.println("In if of contains");
	        		 
	//        		 System.out.println("[" + firstItem + "]");
	//            	 System.out.println("[" + secondItem + "]");
	        		 
	//        		 System.out.println("ATwoItemSet: " + ATwoItemSet);
	        		 
	        		 // Checking Palindrome
	        		 if ( Integer.parseInt(firstItem) > Integer.parseInt(secondItem) ) {
	        			 
	//        			 System.out.println("if in ItemSet write 2 - palindrome");
	        			 
	        			 context.write(new Text(secondItem + " " + firstItem), new IntWritable(1));
	        			 
	        		 } else {
	        			 
	//        			 System.out.println("In else ItemSet write 2 - palindrome");
	        			 
	//        			 context.write(new Text(ATwoItemSet), new IntWritable(1));
	        			 context.write(new Text(firstItem + " " + secondItem), new IntWritable(1));
	        			 
	        		 }
	        	 }
	        	
	//        	System.out.println("Separate words");
	//        	
	//        	System.out.println(separateWords[0]);
	//        	System.out.println(separateWords[1]);
        	
        }
        
        /**
         * End - New part of code
         */
        
        
		
		// ArrayList to hold the final combinations
//		ArrayList<String> getListFromComb = new ArrayList<String>();
		
		// Get the combinations
//		getListFromComb = addedFunctions.getCombinations1(list);
		
//		list.clear();
//		getListFromComb.clear();
		
		
		// Printing all the elements of the ArrayList getListFromComb
//		for (String AString : getListFromComb) {
//		for ( int i = 0; i < getListFromComb.size(); i++ ) {
//					
//			String AString = "";
//			
//			// Very bad idea - still a solution
////			words = AString.split("\\s");
//			words = getListFromComb.get(i).split("\\s");
//					
//			// Checking Palindrome
//			if ( Integer.parseInt(words[0]) > Integer.parseInt(words[1]) ){
//				// Empty string
//				AString = "";
//				AString += words[1];
//				AString += " ";
//				AString += words[0];
//			}
//				
//			// 2-itemSet
//			context.write(new Text(AString), new IntWritable(1));
//		}
		
	}
	
	
	/**
     * Reads the file from distributed cache
     * @param filePath
     */
    private void readFile(Path filePath, Configuration conf) {
		try{
//			System.out.println("readFile: In the try");
			BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
			
//			System.out.println("BufferReader");
//			System.out.println(bufferedReader);
			
//			System.out.println("ReadLine stopword");
			
			String stopWord = null;
			while((stopWord = bufferedReader.readLine()) != null) {
//				System.out.println(stopWord);
				String[] words = stopWord.split("\\s");
				
//				System.out.println("Words in readFile");
//				
//				for ( String newString: words )
//					System.out.println(newString);
				
//				String newWord = words[0].substring(0, words[0].length() - 1);
				
				// putting combinations and values to a hashMap
//				combWithValues.put(words[0], Integer.parseInt(words[1].trim()));
				
				// putting the strings to a ArrayList
				oneItemSetKeys.add(words[0].trim().substring(0, words[0].length() - 1));
				
//				System.out.println("ADDED");
			}
			bufferedReader.close();
		} catch(IOException e) {
			System.err.println("Exception while reading stop words file: " + e.getMessage());
		}
	}

}
