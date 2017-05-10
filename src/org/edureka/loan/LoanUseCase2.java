package org.edureka.loan;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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

public class LoanUseCase2 {

	public static class Map extends Mapper<LongWritable,Text,Text,Text> {
		static int count=0;
		
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException,InterruptedException
		{	
			
			String line=value.toString().replace("\"", "");
			
			/*Considering Term as one year and finding the people whose interest is mare that 5,000*/		
			String parts[]=line.split(",");
			String id=parts[0];
			String loan_amount=parts[2];
			String int_rate=parts[6];
			
			context.write(new Text(id), new Text(loan_amount+"\t"+int_rate));
			
			
		}
	}
	
	public static class Reduce extends
	Reducer<Text, Text, Text, Text> {

  	
  	
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
    	  float loan_amt=0;
    	  float int_rate=0;
    	  float total_interest=0;
			for(Text line:values){
				String record[]=line.toString().split("\t");
				loan_amt=Float.parseFloat(record[0]);
				int_rate=Float.parseFloat(record[1].replaceAll("%",""));
				total_interest=(loan_amt*1*int_rate)/100;
		}
		if(total_interest>=5000)
			context.write(new Text("Id: "+key),new Text("Interest is: "+total_interest));
    	  
		}
	}
	
	
	public static void main(String[] args) throws Exception { 
		
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"LoanUseCase2");
    
		
		//Defining the output value class for the mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(LoanUseCase2.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//Defining the output value class for the mapper
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




