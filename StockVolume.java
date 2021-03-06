//i/p: NYSE.csv
//to find stock volume
//hadoop jar nyse.jar StockVolume /niit/NYSE.csv /niit/out
import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class StockVolume {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  

	    	  try{
	            String[] str = value.toString().split(",");	 
	            long vol = Long.parseLong(str[7]);
	            context.write(new Text(str[1]),new LongWritable(vol));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	 
	public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
	   {
		  //  private LongWritable result = new LongWritable();
		    
		    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		      long sum = 0;
				
		         for (LongWritable val : values)
		         {       	
		        	sum += val.get();      
		         }
  
		     // result.set(sum);		      
		      context.write(key, new LongWritable(sum));
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
		    public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable>//own combiner class
			   {
				    private LongWritable result = new LongWritable();
				    
				    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
				      long sum = 0;
						
				         for (LongWritable val : values)
				         {       	
				        	sum += val.get();      
				         }
				         
				      result.set(sum);		      
				      context.write(key, result);
				      //context.write(key, new LongWritable(sum));
				      
				    }
			
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(StockVolume.class);
		    job.setMapperClass(MapClass.class);
		//  job.setCombinerClass(CombinerClass.class);//combine class done at mapper part
		   job.setReducerClass(ReduceClass.class);
		   job.setNumReduceTasks(3);//partioner is 3...combner nd partioner are not runed at a time
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(SequenceFileOutputFormat.class);//two lines gives text to sequenec o/p format
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	   }
