package com.vijet.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * This program computes the Mean Min temperature and Mean max temperature by station.
 * There is no combiner in the program.
 */
public class MapReduceNoCombiner {
	
	public static final String MIN_TEMP = "TMIN";
	public static final String MAX_TEMP = "TMAX";
	public static final String NOT_DEFINED = "Not Defined";

	/**
	 * Bean class that stores the Average Minimum Temperature per Station.
	 */
	public static class StationWritable implements Writable{
		public Text avgMinTemp;
		public Text avgMaxTemp;

		public StationWritable() {
			avgMinTemp = new Text(NOT_DEFINED);
			avgMaxTemp = new Text(NOT_DEFINED);
		}		

		@Override
		public void write(DataOutput out) throws IOException {
			avgMinTemp.write(out);
			avgMaxTemp.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			avgMinTemp.readFields(in);
			avgMaxTemp.readFields(in);
		}

	}
	
	/**
	 * Mapper class that emits the station and Station iff the type is of TMIN or TMAX.
	 */
	public static class MapReduceNoCombinerMapper extends Mapper<Object, Text, Text, StationWritable>{
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] stationInfo = value.toString().split(",");
			StationWritable tempStn = new StationWritable();
			if(stationInfo[2].equals(MIN_TEMP)){
				tempStn.avgMinTemp = new Text(stationInfo[3]);
				context.write(new Text(stationInfo[0]), tempStn);
			}
			else if(stationInfo[2].equals(MAX_TEMP)){
				tempStn.avgMaxTemp = new Text(stationInfo[3]);
				context.write(new Text(stationInfo[0]), tempStn);
			}
		}
	}
	
	/**
	 * Reducer class that aggregates TMIN and TMAX per station and computes respective average.
	 */
	public static class MapReduceNoCombinerReducer extends Reducer<Text,StationWritable,Text,NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<StationWritable> valueIter, Context context) throws IOException, InterruptedException {
			long minCount = 0;
			long maxCount = 0;
			double minSum = 0.0;
			double maxSum = 0.0;
			for (StationWritable stationWritable : valueIter) {
				if(!stationWritable.avgMaxTemp.toString().equals(NOT_DEFINED)){
					maxCount+=1;
					maxSum+=Double.valueOf(stationWritable.avgMaxTemp.toString());
				}
				if(!stationWritable.avgMinTemp.toString().equals(NOT_DEFINED)){
					minCount+=1;
					minSum+=Double.valueOf(stationWritable.avgMinTemp.toString());
				}
			}
			StationWritable stnWritable = new StationWritable();
			if(maxCount!=0){
				stnWritable.avgMaxTemp = new Text(String.valueOf(maxSum/maxCount));
			}if(minCount!=0){
				stnWritable.avgMinTemp = new Text(String.valueOf(minSum/minCount));
			}
			context.write(new Text(key+", "+stnWritable.avgMinTemp+","+stnWritable.avgMaxTemp), NullWritable.get());
		}
	}
	
	/**
	 * Driver class
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length < 2){
			System.out.println("Invalid Format <Input-File> [<Input-File] <OutputFile>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "MapReduceNoCombiner");
		job.setJarByClass(MapReduceNoCombiner.class);
		
		job.setMapperClass(MapReduceNoCombinerMapper.class);
		job.setReducerClass(MapReduceNoCombinerReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		for(int i = 0 ; i < args.length-1;i++){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		
		FileOutputFormat.setOutputPath(job,new Path(args[args.length-1]));
		
		job.waitForCompletion(true);
		
	}
}
