//I/p data: Project - Retail Store Data D01,D02,D11,D12
//hadoop jar comp.jar B1 /niit/company /niit1/b1

//B Find out the top 4 or top 10 product being sold in the monthly basis and in all the 4 months.. Criteria for top should be sales amount.
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class B1 {
	
	public static class BMapperClass extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			 try{
		            String[] str = value.toString().split(";");
		            long sales = Long.parseLong(str[8]);
		            context.write(new Text(str[5].trim()), new LongWritable(sales));
		         }
		         catch(Exception e)
		         {
		            System.out.println(e.getMessage());
		         }
		}
	}

	
	public static class BReducerClass extends Reducer<Text,LongWritable,Text,LongWritable>
	{
		 private LongWritable result = new LongWritable();
		 public void reduce(Text key,Iterable<LongWritable> value, Context context) throws IOException, InterruptedException
		{
			long sum=0;
			for(LongWritable  val : value)
			{
				sum+=val.get();
			}
			  result.set(sum);	
			context.write(key, result);
		}
	}
	
	
	public static class SortMapper extends Mapper<LongWritable,Text,LongWritable, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valueArr = value.toString().split("\t");
			context.write(new LongWritable(Long.parseLong(valueArr[1])), new Text(valueArr[0]));
		}
	}

	public static class SortReducer extends Reducer<LongWritable,Text,Text,LongWritable>
	{
		int counter =1;
		public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			for(Text val : value)
			{
				if(counter<11)
				{
				context.write(new Text(val), key);
			counter=counter+1;
		}
				}
		}
	}
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf,"total sales calculation for each product");
		job1.setJarByClass(B1.class);
		job1.setMapperClass(BMapperClass.class);
		job1.setReducerClass(BReducerClass.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);
		//MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CustMapper.class);
		//MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, StoreMapper.class);
		Path outputPath1 = new Path("FirstMapper");
		FileInputFormat.addInputPath(job1, new Path(args[0]));
FileOutputFormat.setOutputPath(job1, outputPath1);
		FileSystem.get(conf).delete(outputPath1, true);
		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf,"sorting on highest sales");
		job2.setJarByClass(B1.class);
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		job2.setSortComparatorClass(DecreasingComparator.class);
job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		FileSystem.get(conf).delete(new Path(args[1]), true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
				
	
	}
		
	}

	
	