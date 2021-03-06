package apriori;

import java.io.IOException;
//import java.io.BufferedReader;
//import java.io.InputStreamReader;
//
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileStatus;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.FileSystem;

import utils.addedFunctions;

public class aprioriMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		// Time measurement
		long startTime = System.currentTimeMillis();
		long endTime;
		
		// minimum support for the association
		Double MIN_SUPPORT_PERCENT;
		// maximum transaction count 
		Integer MAX_NUM_TXNS;
		
		// Minimum Confidence
		Double min_conf;
		
		// Number of Reducer 
		// 0 is the default
		Integer numOfReducer = 10;
		
		// configuration
		Configuration conf = new Configuration();
		
		// Memory Increase
//		conf.set("mapreduce.map.memory.mb", "8192");
		conf.set("mapreduce.map.memory.mb", "5630");
		conf.set("mapreduce.map.java.opts", "-Xmx5600m");
		
		// argument 3 for minimum support
		if ( args[2] != null ) {
			MIN_SUPPORT_PERCENT = Double.parseDouble(args[2]);
			conf.set("minSup", Double.toString(MIN_SUPPORT_PERCENT));
		}
		
		// argument 4 for maximum transaction
		if ( args[3] != null ) {
			MAX_NUM_TXNS = Integer.parseInt(args[3]);
			conf.setInt("numTxns", MAX_NUM_TXNS);
		}
		
		// argument 4 for maximum transaction
		if ( args[4] != null ) {
			min_conf = Double.parseDouble(args[4]);
			conf.set("minConf", Double.toString(min_conf));
		}
		
		if ( args[5] != null ) {
			numOfReducer = Integer.parseInt(args[5]);
		}
		
		// create the job
//        Configuration conf = new Configuration();
        Job job = new Job(conf, "apriori");
        job.setJarByClass(aprioriMain.class);
        
//        System.out.println("block size: " + conf.get("dfs.blocksize"));
        
        // Distributed cache to pass the main input file
//        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());

        // Add the required configurations

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setInputFormatClass(TextInputFormat.class);

        // Submits the job

        job.setMapperClass(aprioriMapper1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        job.setNumReduceTasks(10);
        job.setNumReduceTasks(numOfReducer);
        job.setReducerClass(aprioriReducer1.class);
        
//        setMaxInputSplitSize(job, 128*1024);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);

        // output in a file & stored in HDFS
        job.setOutputFormatClass(TextOutputFormat.class);
        
        /**
         * DELETE output folder if exists
         */
        addedFunctions.deleteOutputFolder(args[1], conf);
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean succeeded = job.waitForCompletion(true);

        if( !succeeded ){
            throw new IllegalStateException("Job failed");
        }
        
        // CopyMerge
        
        // DELETE output folder if exists - not folder
        addedFunctions.deleteOutputFolder(args[1] + "-output", conf);
        
        if ( addedFunctions.copyMergeFiles(conf, args[1], args[1] + "-output") )
        	System.out.println("copyMerge successful");
        else
        	System.out.println("copyMerge not happened");
        
        /*
         * 
         * Job 2 
         */
        
        Job job2 = new Job(conf, "apriori2");
        job2.setJarByClass(aprioriMain.class);
        
        // Distributed cache to pass the main input file
        DistributedCache.addCacheFile(new Path(args[1] + "-output").toUri(), job2.getConfiguration());

        // Add the required configurations

        FileInputFormat.addInputPath(job2, new Path(args[0]));

        job2.setInputFormatClass(TextInputFormat.class);

        // Submits the job

        job2.setMapperClass(aprioriMapper2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

//        job2.setNumReduceTasks(10);
        job2.setNumReduceTasks(numOfReducer);
        job2.setReducerClass(aprioriReducer2.class);
        
//        job2.setMapOutputKeyClass(Text.class);
//        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        
        /**
         * DELETE output folder if exists
         */
        addedFunctions.deleteOutputFolder(args[1] + "-1", conf);

        // output in a file & stored in HDFS
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "-1"));

        boolean succeeded2 = job2.waitForCompletion(true);

        if( !succeeded2 ){
            throw new IllegalStateException("Job2 failed");
        }
        
        
        /**
         * CopyMerge - 2nd Time
         */
        
        // DELETE output folder if exists - not folder
        addedFunctions.deleteOutputFolder(args[1] + "-outputFinal", conf);
        
        if ( addedFunctions.copyMergeFiles(conf, args[1] + "-1", args[1] + "-outputFinal") )
        	System.out.println("copyMerge successful");
        else
        	System.out.println("copyMerge not happened");
        
        
        /**
         * 
         * Job 3
         */
        
        Job job3 = new Job(conf, "apriori3");
        job3.setJarByClass(aprioriMain.class);
        
        // Distributed cache to pass the main input file
        DistributedCache.addCacheFile(new Path(args[1] + "-output").toUri(), job3.getConfiguration());
        DistributedCache.addCacheFile(new Path(args[1] + "-outputFinal").toUri(), job3.getConfiguration());
        // Add the required configurations

        FileInputFormat.addInputPath(job3, new Path(args[1] + "-1"));

        job3.setInputFormatClass(TextInputFormat.class);

        // Submits the job

        job3.setMapperClass(aprioriMapper3.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setNumReduceTasks(0);
//        job2.setReducerClass(aprioriReducer1.class);
        
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

//        job2.setOutputKeyClass(Text.class);
//        job2.setOutputValueClass(LongWritable.class);
        
        /**
         * DELETE output folder if exists
         */
        addedFunctions.deleteOutputFolder(args[1] + "-2", conf);

        // output in a file & stored in HDFS
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "-2"));

        boolean succeeded3 = job3.waitForCompletion(true);

        if( !succeeded3 ){
            throw new IllegalStateException("Job3 failed");
        }
        
        
        /**
         * End of job3
         */
        
        // Time measurement
        endTime = System.currentTimeMillis();
//        System.out.println("start: " + startTime);
//        System.out.println("End: " + endTime);
		System.out.println("Total time taken = " + (endTime - startTime) );
		
	}
	
}
