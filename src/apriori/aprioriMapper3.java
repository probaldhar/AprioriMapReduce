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

//import utils.addedFunctions;

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

		// Not adding 1-itemset
    	if ( splitWords.length > 2 ) {
    		
//    		System.out.println("TextToString: [" + textToString + "]");
//    		System.out.println("splitWords1: " + splitWords[1].substring(0, splitWords[1].length()-1));
    		
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
    		float support = (float) Integer.parseInt(words[1].trim());
    		
//    		System.out.println("Support: " + support );
    		
    		// Calculating confidence for this itemset ( 2 itemset )
    		float confidence = support/firstValue;
    		
//    		System.out.println("conf: " + confidence );
    		
    		float lift = support/(firstValue * secondValue);
    		
//    		System.out.println("lift: " + lift );
         
    		context.write(new Text(words[0] + ","), new Text(String.valueOf(support) + ", " + String.valueOf(confidence) + ", " + String.valueOf(lift)) );
            
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


