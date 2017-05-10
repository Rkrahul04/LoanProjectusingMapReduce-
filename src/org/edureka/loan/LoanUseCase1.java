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

/*Finding the list of people with particular grade who have taken loan */

public class LoanUseCase1 {

	public static class Map extends Mapper<LongWritable,Text,Text,Text> {
		static int count=0;
		
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException,InterruptedException
		{	
			
			String line=value.toString().replace("\"", "");
						
			String parts[]=line.split(",");
		
			String id=parts[0];
			//String member_id=parts[1];
			String grade=parts[8];
			String loan_amount=parts[2];
			if(grade.equals("A"))
		 context.write(new Text("In grade:"+grade), new Text("Person with id:"+id+" as Taken the loan amont of:"+loan_amount));
			
		}
	}
	
	

  	
  	
     
	
	
	public static void main(String[] args) throws Exception { 
		
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"LoanUseCase1");
   
		
		//Defining the output key and value class for the mapper
		 job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setJarByClass(LoanUseCase1.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
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




