
        
import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Orders {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
   
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] token = line.split("\\|");
        int cust_key = Integer.valueOf(token[1]);
        
        double price = Double.valueOf(token[3]);
        
        context.write(new IntWritable(cust_key), new DoubleWritable(price));
        	
        
        
    }
 }
  
        
 public static class Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable ,Text> {

    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) 
      throws IOException, InterruptedException {
    	
    	double sum = 0;
    	
    	for(DoubleWritable value:values)
    	{
    		sum=sum+value.get();
    		
    	}
      
        context.write(key, new Text("Total Price is:"+sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "Orders");
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
//    job.setInputFormatClass(In.class);
//    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}

