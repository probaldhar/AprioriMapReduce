package apriori;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
//import java.util.ArrayList;

//import java.io.InputStreamReader;
import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.util.ReflectionUtils;

import utils.addedFunctions;

/**
 * 
 * All calculations are in here
 * 
 * Details of the constant used in this file
 * 
 * firstValue 		- 
 * secondValue 		- 
 * 
 * support			-
 * confidence		-
 * lift				-
 * 
 * support2ndRule	-
 * confidence2ndRule	-
 * lift2ndRule		-
 * 
 * support3rdRule	-
 * confidence3rdRule	-
 * lift3rdRule		-
 * 
 * support4thRule	-
 * confidence4thRule	-
 * lift4thRule		-
 * 
 * @author 	Probal Chandra Dhar
 * @see		Other Files before seeing this file
 *
 */

public class aprioriMapper3 extends Mapper<LongWritable,Text,Text,Text>{
	
	// HashMap to get the input file
//	private Set<String> stopWords = new HashSet<String>();
	
	private HashMap<String, Integer> combWithValues = new HashMap<String, Integer>();
//	
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
	    		
	    	} catch(IOException ex) {
	    		System.err.println("Exception in mapper setup: " + ex.getMessage());
	    	}
    }
	
    @Override
    protected void map(LongWritable offset, Text input, Context context) throws IOException, InterruptedException {
        // the required map code should be added here
    	
    		// Assuming for now
    		float min_conf = 0.5f;
    		Integer numTxns = context.getConfiguration().getInt("numTxns", 2);
    	
	    	/**
	    	 * Main Input
	    	 */
	    	String textToString = input.toString();
	    	
	    	/**
	    	 * Splitting main input : by comma
	    	 * words -> generated 2 Frequent itemSet
	    	 */
	    	String[] words = textToString.split(",");
	    	
	//    	System.out.println("words length: " + words.length);
	    	
	    	/**
	    	 * Splitting main input : by space
	    	 */
	    	String [] splitWords = textToString.split("\\s");
	    	
	//    	System.out.println("splitWords length: " + splitWords.length);
			
	//		for ( String aString: splitWords )
	//			System.out.println("[" + aString + "]");
	
			// Not adding 1-itemset - no 1-itemSet NOW
	    	if ( splitWords.length > 2 ) {
	    		
	//    		System.out.println("TextToString: [" + textToString + "]");
	//    		System.out.println("splitWords1: " + splitWords[1].substring(0, splitWords[1].length()-1));
	    		
	    		//
	    		float firstValue, secondValue;
	    		
	    		// get the value of the key
	    		if ( combWithValues.containsKey(splitWords[0]))
	    			firstValue = (float) combWithValues.get(splitWords[0]);
	    		else if ( combWithValues.containsKey(splitWords[0] + ","))
	    			firstValue = (float) combWithValues.get(splitWords[0] + ",");
	    		else
	    			firstValue = -1;
	    		
	//    		System.out.println("First Key: " + splitWords[0] + " and value: " + firstValue);
	    		
	    		// trim the tail of ','
	    		String secondKey = splitWords[1].substring(0, splitWords[1].length()-1);
	    		
	    		// get the value of the second key
	    		if ( combWithValues.containsKey(secondKey) )
	    			secondValue = (float) combWithValues.get(secondKey);
	    		else if ( combWithValues.containsKey(secondKey + ",") )
	    			secondValue = (float) combWithValues.get(secondKey + ",");
	    		else
	    			secondValue = -1;
	    		
	//    		System.out.println("Second Key: " + secondKey + " and value: " + secondValue);
	    		
	    		// frequency of the itemset in the dataset
//	    		float support = (float) Integer.parseInt(words[1].trim());
	    		float support = (float) Integer.parseInt(words[1].trim()) / (float) numTxns;
	    		
	//    		System.out.println("Support: " + support );
	    		
	    		// Calculating confidence for this itemset ( 2 itemset )
	    		float confidence = support/firstValue;
	    		
	//    		System.out.println("conf: " + confidence );
	    		
	    		// getting the support
	    		float supportA = addedFunctions.getSupport(firstValue, numTxns);
	    		float supportB = addedFunctions.getSupport(secondValue, numTxns);
	    		
//	    		float lift = support/(firstValue * secondValue);
	    		float lift = support/(supportA * supportB);
	    		
	//    		System.out.println("lift: " + lift );
	    		
	    		/**
	    		 * FOR RULE A -> B
	    		 */
	         
	    		context.write(new Text(words[0] + ","), new Text(String.valueOf(support) + ", " + String.valueOf(confidence) + ", " + String.valueOf(lift)) );
	    		
	    		/**
	    		 * FOR RULE A -> !B
	    		 */
	    		
	    		// Rule number (6)
//	    		float support2ndRule = firstValue - support;
	    		float support2ndRule = supportA - support;
	    		
	    		// Rule number (7)
	    		float confidence2ndRule = 1 - confidence;
	    		
	    		// Rule number (4)
//	    		float lift2ndRule = support2ndRule/(firstValue * ( 1 - secondValue ));
	    		float lift2ndRule = support2ndRule/(supportA * ( 1 - supportB ));
	    		
	    		// Checking for negative rule
	    		if ( confidence2ndRule > min_conf )
	    			context.write(new Text("A -> !B " + words[0] + ","), new Text(String.valueOf(support2ndRule) + ", " + String.valueOf(confidence2ndRule) + ", " + String.valueOf(lift2ndRule) + " valid negative") );
	    		else 
	    			context.write(new Text("A -> !B " + words[0] + ","), new Text(String.valueOf(support2ndRule) + ", " + String.valueOf(confidence2ndRule) + ", " + String.valueOf(lift2ndRule)) );
	    		
	    		
	    		/**
	    		 * FOR RULE !A -> B
	    		 */
	    		
	    		// Rule number (8)
//	    		float support3rdRule = secondValue - support;
	    		float support3rdRule = supportB - support;
	    		
//	    		System.out.println("SupportB" + supportB);
//	    		System.out.println("Support" + support);
//	    		System.out.println("Words" + words[0]);
	    		
	    		// Rule number (9)
//	    		float confidence3rdRule = support3rdRule/( 1 - firstValue);
	    		float confidence3rdRule = support3rdRule/( 1 - supportA);
	    		
	    		// Rule number (4)
//	    		float lift3rdRule = support3rdRule/( ( 1 - firstValue ) * secondValue);
	    		float lift3rdRule = support3rdRule/( ( 1 - supportA ) * supportB);
	    		
	    		// Checking for negative rule
	    		if ( confidence3rdRule > min_conf )
	    			context.write(new Text("!A -> B " + words[0] + ","), new Text(String.valueOf(support3rdRule) + ", " + String.valueOf(confidence3rdRule) + ", " + String.valueOf(lift3rdRule) + " valid negative") );
	    		else 
	    			context.write(new Text("!A -> B " + words[0] + ","), new Text(String.valueOf(support3rdRule) + ", " + String.valueOf(confidence3rdRule) + ", " + String.valueOf(lift3rdRule)) );
	    		
	    		
	    		/**
	    		 * FOR RULE !A -> !B
	    		 */
	    		
	    		// Rule number (10)
//	    		float support4thRule = 1 - firstValue - secondValue - support;
	    		float support4thRule = 1 - supportA - supportB + support;
	    		
	    		// Rule number (11)
//	    		float confidence4thRule = support4thRule/( 1 - firstValue);
	    		float confidence4thRule = support4thRule/( 1 - supportA);
	    		
	    		// Rule number (4)
//	    		float lift4thRule = support4thRule/( ( 1 - firstValue ) * ( 1 - secondValue ) );
	    		float lift4thRule = support4thRule/( ( 1 - supportA ) * ( 1 - supportB ) );
	    		
	    		// Checking for negative rule
	    		if ( confidence4thRule > min_conf )
	    			context.write(new Text("!A -> !B " + words[0] + ","), new Text(String.valueOf(support4thRule) + ", " + String.valueOf(confidence4thRule) + ", " + String.valueOf(lift4thRule) + " valid negative") );
	    		else 
	    			context.write(new Text("!A -> !B " + words[0] + ","), new Text(String.valueOf(support4thRule) + ", " + String.valueOf(confidence4thRule) + ", " + String.valueOf(lift4thRule)) );
	            
	    	}
    	
    	
    }
    
    /**
     * Reads the file from distributed cache
     * @param filePath
     */
    private void readFile(Path filePath, Configuration conf) {
		try{
//			System.out.println("readFile: In the try");
//			FileSystem fs = FileSystem.get(conf);
//			Path getPath = filePath;
//			BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
//			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
			BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
			
//			System.out.println("BufferReader");
//			System.out.println(bufferedReader);
			
//			System.out.println("ReadLine stopword");
			
			String stopWord = null;
			while((stopWord = bufferedReader.readLine()) != null) {
//				System.out.println(stopWord);
				String[] words = stopWord.split(",");
				
//				for ( String newString: words )
//					System.out.println(newString);
				
//				String newWord = words[0].substring(0, words[0].length() - 1);
				
				// putting combinations and values to a hashMap
				combWithValues.put(words[0], Integer.parseInt(words[1].trim()));
				
				// putting the strings to a hashSet
//				stopWords.add(stopWord);
				
//				System.out.println("ADDED");
			}
			bufferedReader.close();
		} catch(IOException e) {
			System.err.println("Exception while reading stop words file: " + e.getMessage());
		}
	}
	    
}


