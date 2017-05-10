package org.edureka.loan;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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

/*Highest loan amount given in 2013 with that Employee id and Employee’s annual income. */

public class LoanUseCase5 {

	public static class Map extends Mapper<LongWritable,Text,Text,Text> {
		static int count=0;
		
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException,InterruptedException
		{	
			
			String line=value.toString().replace("\"", "");
			String parts[]=line.split(",");
			
			String id=parts[0];
			String an_income=parts[13];
			String loan_amount=parts[2];
			String year=parts[15].replaceAll("[^\\d]","");
			if(StringUtils.isNumeric(year)&& year.equals("2013"))
			context.write(new Text(year),new Text(id+","+loan_amount+","+an_income));
			
		}
	}
	
	public static class Reduce extends
	Reducer<Text, Text, Text, Text> {

  	
  	
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
    	  int maxLoan=0;
    	  String id="";
    	  String income="";
    	  for(Text line:values){
    		  String record[]=line.toString().split(",");
    		  id=record[0];
    		  int loan=Integer.parseInt(record[1]);
    		  if(loan>maxLoan)
    			  maxLoan=loan;
    		  income=record[2];
    	 }
    	  
    	  context.write(new Text("In the year:"+key+" Employee with Id:"+id+" and annual income:"+income+" was issued loan amount:"+maxLoan),null);
		}
	}
	
	
	public static void main(String[] args) throws Exception { 
		
		Configuration conf= new Configuration();
				
		Job job = new Job(conf,"LoanUseCase5");
		job.setMapOutputKeyClass(Text.class);
		
			
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(LoanUseCase5.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
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




