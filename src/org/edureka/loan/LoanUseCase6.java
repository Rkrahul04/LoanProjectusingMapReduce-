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

/*Get the total number of loans with loan id and loan amount which are all having loan status as ‘Late’. */

public class LoanUseCase6 {

	public static class Map extends Mapper<LongWritable,Text,Text,Text> {
		static int count=0;
		
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException,InterruptedException
		{	
			
			String line=value.toString().replace("\"", "");
			String parts[]=line.split(",");
			
			String id=parts[0];
			String loan_amt=parts[2];
			String status=parts[16].toLowerCase();
			if(status.contains("late"))
				context.write(new Text(status),new Text(id+","+loan_amt));
		}
	}
	
	public static class Reduce extends 	Reducer<Text, Text, Text, Text> {

  	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
  		  int count=0; 
        for(Text line:values){
        	String parts[]=line.toString().split(",");
        	String id=parts[0];
        	String loan_amt=parts[1];
          
        context.write(new Text("id:"+id+" Amount : "+loan_amt), null);	
        count++;
        	
      }
        
        context.write(new Text("Total count of employes whose loan status is late :"+count), null);
    	 
		}
	}
	
	
	public static void main(String[] args) throws Exception { 
		
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"LoanUseCase6");
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(LoanUseCase6.class);
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




