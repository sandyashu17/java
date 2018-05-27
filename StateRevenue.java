//i/p pos1,pos2 and store master 
//hadoop jar comp.jar StateRevenue /niit/posdocs /niit/store_master /niit/mapside3

//ques: state revenue( map side join)
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//import StockVolume.ReduceClass;

public class StateRevenue {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
        
		
		private Map<String, String> abMap = new HashMap<String, String>();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
		  
		    
		    FileSystem fs = FileSystem.get(context.getConfiguration());		    
		
			if (p.getName().equals("store_master")) {

					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));

					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(",");
						String store_id = tokens[0];
						String store_state = tokens[2];
						abMap.put(store_id, store_state);
						line = reader.readLine();
					}
					reader.close();
				}
			
		
			
			if (abMap.isEmpty()) {
				throw new IOException("MyError:Unable to load store data");
			}

		
		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	
        	String row = value.toString();//reading the data from Employees.txt
        	String[] tokens = row.split(",");
        	String store_id = tokens[0];
        	String state = abMap.get(store_id);
        	
        	int sales =( Integer.parseInt(tokens[2]) * Integer.parseInt(tokens[3])); 
        	//outputKey.set(row);
        	//outputValue.set(sal_desig);
      	  	context.write(new Text(state),new IntWritable(sales));
        }  
}
	public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		    private IntWritable result = new IntWritable();
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      int sum = 0;
				
		         for (IntWritable val : values)
		         {       	
		        	sum += val.get();      
		         }

		     result.set(sum);		      
		      context.write(key, result);
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(StateRevenue.class);
    job.setJobName("State wise sales");
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(ReduceClass.class);
    job.addCacheFile(new Path(args[1]).toUri());
    //job.addCacheFile(new Path(args[1]).toUri());
 //job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    job.waitForCompletion(true);
    
    
  }
}