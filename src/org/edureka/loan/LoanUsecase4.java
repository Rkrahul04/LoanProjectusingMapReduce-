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

//Get maximum number of loan given to which grade users.(A-G)

public class LoanUsecase4 {

	public static class Map extends Mapper<LongWritable,Text,Text,Text> {
		static int grade_A_count=0;
		static int grade_B_count=0;
		static int grade_C_count=0;
		static	int grade_D_count=0;
		static	int grade_E_count=0;
		static	int grade_F_count=0;
		static	int grade_G_count=0;
		
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException,InterruptedException
		{	
			
			String line=value.toString().replace("\"", "");
			
			String parts[]=line.split(",");
			
			
			String grade=parts[8];
			
			if(grade.equals("A")){
				grade_A_count++;
				context.write(new Text("1"),new Text(grade+"\t"+grade_A_count));
			}
			else if(grade.equals("B")){
				grade_B_count++;
				context.write(new Text("1"),new Text(grade+"\t"+grade_B_count));
			}
			else if(grade.equals("C")){
				grade_C_count++;
				context.write(new Text("1"),new Text(grade+"\t"+grade_C_count));
			}
			else if(grade.equals("D")){
				grade_D_count++;
				context.write(new Text("1"),new Text(grade+"\t"+grade_D_count));
			}
			else if(grade.equals("E")){
				grade_E_count++;
				context.write(new Text("1"),new Text(grade+"\t"+grade_E_count));
			}
			else if(grade.equals("F")){
				grade_F_count++;
				context.write(new Text("1"),new Text(grade+"\t"+grade_F_count));
			}
			else if(grade.equals("G")){
				grade_G_count++;
				context.write(new Text("1"),new Text(grade+"\t"+grade_G_count));
			}
		}
	}
	
	public static class Reduce extends
	Reducer<Text, Text, Text, Text> {

  	
  	
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
    	  String grade=null;
    	  int max=0;
			for(Text record:values){
				String parts[]=record.toString().split("\t");
				int temp=Integer.parseInt(parts[1]);
				
				if(temp>max) {
					max=temp;
					grade=parts[0];
				}
				
				
			}
			
			context.write(new Text("Grade "+grade+" as the maximum count with :"+max), null);
    	  
		}
	}
	
	
	public static void main(String[] args) throws Exception { 
		
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"LoanUsecase4");
		job.setMapOutputKeyClass(Text.class);
					
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(LoanUsecase4.class);
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




