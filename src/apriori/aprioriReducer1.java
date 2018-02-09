package apriori;

import java.io.IOException;
//import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//import utils.addedFunctions;

public class aprioriReducer1 extends Reducer<Text, IntWritable, Text, LongWritable>{
	
	@Override
    protected void reduce(Text itemset, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{
		
		// Initial ArrayList
//     	ArrayList<String> list = new ArrayList<String>();

     	// counting the frequency
        long total = 0;
        
        for (IntWritable value : values) {
        	total += value.get();
		}
        
        String itemsetIds = itemset.toString();
        
        // getting the minimum support & maximum number of transactions
//        Double minSup = Double.parseDouble(context.getConfiguration().get("minSup"));
//        Integer numTxns = context.getConfiguration().getInt("numTxns", 2);
        
        // getting the "actual" support in respect to the maximum transaction
//        total = total / (long) numTxns;
        
        // Check if the total is greater or equal than minimum support
//        if ( addedFunctions.hasMinSupport(minSup, numTxns, total) ) 
//        	context.write(new Text(itemsetIds + ","), new LongWritable(total));
        	context.write(new Text(itemsetIds), new LongWritable(total));

    }

}
