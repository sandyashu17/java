//(reduce side join)
// i/p custs.txt and trnx1.txt
// hadoop jar state.jar ReduceSideJoin /niit/custs.txt /niit/txns1.txt /niit1/ReduceSide1


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceSideJoin {
	
	public static class custMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String valueArr[] = value.toString().split(",");
			String custID = valueArr[0];
			String fname= valueArr[1];
			//String c_custOcc = "c" + "," + custOcc;
			context.write(new Text(custID), new Text("custs\t" +fname));
		}
	}
	public static class salesMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String valueArr[] = value.toString().split(",");
			String custID = valueArr[2];
			String amount= valueArr[3];
			//String c_custOcc = "c" + "," + custOcc;
			context.write(new Text(custID), new Text("txns\t" +amount));
		}
	}

	

	public static class ReduceJoinReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			String name = "";
			double total = 0.0;
			int count=0;
			for(Text val : value)
			{
				String valArr[] = val.toString().split("\t");
				
				if(valArr[0].equals("txns"))
				{
					count++;
					total+=Float.parseFloat(valArr[1]);
					
				}
				else if(valArr[0].equals("custs"))
				{
					name = valArr[1];
				}
			}
		String str=String.format("%d\t%f",count,total);//d integer f double
			context.write(new Text(name), new Text(str));
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
	
		 Configuration conf = new Configuration();
	    	Job job= Job.getInstance(conf);
	    job.setJarByClass(ReduceSideJoin.class);
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, custMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, salesMapper.class);
		job.setReducerClass(ReduceJoinReducer.class);
	//job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	   job.setMapOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	   // Path outputPath = new Path(args[2]);
		//FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));

//	    FileOutputFormat.setOutputPath(job, outputPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
		
	}

	
	