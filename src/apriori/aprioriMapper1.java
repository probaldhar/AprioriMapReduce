package apriori;

//import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//import utils.addedFunctions;

public class aprioriMapper1 extends Mapper<LongWritable,Text,Text,IntWritable>{
	
    @Override
    protected void map(LongWritable offset, Text input, Context context)
    throws IOException, InterruptedException {
        // the required map code should be added here
    	
    	String textToString = input.toString();

        String[] words = textToString.split("\\s");
        
        // Initial ArrayList
//     	ArrayList<String> list = new ArrayList<String>();

        for ( int i = 0; i < words.length; i++ ) {
        	
        	// adding all 1-itemset to the the list
//        	list.add(words[i]);

        	// 1-itemSet
            context.write(new Text(words[i]), new IntWritable(1));

        }
        
        // ArrayList to hold the final combinations
//		ArrayList<String> getListFromComb = new ArrayList<String>();
		
//		System.out.println("Debugging start");
//		for(int i = 0; i < list.size() - 1; i++)
//			System.out.println(list.get(i));
//		System.out.println("Debugging end");
			
		// Get the combinations
//		getListFromComb = addedFunctions.getCombinations(list);
//		getListFromComb = addedFunctions.getCombinations1(list);
		
//		System.out.println("getListFromComb start");
//		for(int i = 0; i < getListFromComb.size() - 1; i++)
//			System.out.println(getListFromComb.get(i));
//		System.out.println("getListFromComb end");
		
		// Printing all the elements of the ArrayList getListFromComb
//		for (String AString : getListFromComb) {
//			
//			// Very bad idea - still a solution
//			words = AString.split("\\s");
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
	    
}




