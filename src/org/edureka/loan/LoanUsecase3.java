package org.edureka.loan;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

//Finding the list of people with having loan amount more than certain value 

public class LoanUsecase3 {

	public static class Map extends Mapper<LongWritable,Text,Text,Text> {
		
		
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException,InterruptedException
		{	
			
			String line=value.toString().replace("\"", "");
			
			String parts[]=line.split(",");
			
			String id=parts[0];
			String grade=parts[8];
			int loan_amt=Integer.parseInt(parts[2]);
//List of people with loan amount more than 10,000
			if(loan_amt>10000)
				context.write(new Text("Person with id: "+id+"Has Taken the loan amount of :"+loan_amt ),null);
			
		}
	}
	
	
	
	public static void main(String[] args) throws Exception { 
		
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"LoanUsecase3");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(LoanUsecase3.class);
		job.setMapperClass(Map.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		Path outputPath = new Path(args[1]);
	
		 	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, outputPath);
	   		
		//deleting the output path automatically from hdfs so that we don't have delete it explicitly
			
		outputPath.getFileSystem(conf).delete(outputPath);
			
		//exiting the job only if the flag value becomes false
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	}




