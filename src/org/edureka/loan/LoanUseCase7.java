package org.edureka.loan;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

/*Average loan interest rate with 60 month term and 36 month term.*/

public class LoanUseCase7 {

	public static class Map extends Mapper<LongWritable,Text,Text,FloatWritable> {
				
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException,InterruptedException
		{	
			
			String line=value.toString().replace("\"", "");
			String parts[]=line.split(",");
			String term=parts[5];
			float int_rate=Float.parseFloat(parts[6].replaceAll("%", ""));
		context.write(new Text(term), new FloatWritable(int_rate));	
			
		}
	}
	
	public static class Reduce extends
	Reducer<Text, FloatWritable, Text, FloatWritable> {

  	
  	
      @Override
      public void reduce(Text key, Iterable<FloatWritable> values, Context context)
		throws IOException, InterruptedException {
    	  
    	  float total_int_rate=0;
    	  float avg_int_rate=0;
    	  int count=0;
    	  for(FloatWritable record:values){
    		  total_int_rate+=record.get();
    		  count++;
    	  }
    	  
    	  avg_int_rate=total_int_rate/count;
    	  context.write(new Text("Avarage Interest rate for term : "+key), new FloatWritable(avg_int_rate));
      }
	}
	
	
	public static void main(String[] args) throws Exception { 
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"LoanUseCase7");
   
		
		
		job.setJarByClass(LoanUseCase7.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//Defining the output key and value class for the Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		//Defining the output key and value class for the Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
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




