package com.vijet.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * This program computes the Mean Min temperature and Mean max temperature by station.
 * There is combiner in the program.
 */
public class MapReduceWithCombiner {
	public static final String MIN_TEMP = "TMIN";
	public static final String MAX_TEMP = "TMAX";
	public static final String NOT_DEFINED = "Not Defined";
	/**
	 * Bean class that stores the Average Minimum Temperature per Station.
	 */
	public static class StationWritable implements Writable{
		public Text sumMinTemp;
		public Text sumMinCount;
		public Text sumMaxTemp;
		public Text sumMaxCount;

		public StationWritable() {
			sumMinTemp = new Text(NOT_DEFINED);
			sumMinCount = new Text(NOT_DEFINED);
			sumMaxTemp = new Text(NOT_DEFINED);
			sumMaxCount = new Text(NOT_DEFINED);
		}		

		@Override
		public void write(DataOutput out) throws IOException {
			sumMinTemp.write(out);
			sumMinCount.write(out);
			sumMaxTemp.write(out);
			sumMaxCount.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			sumMinTemp.readFields(in);
			sumMinCount.readFields(in);
			sumMaxTemp.readFields(in);
			sumMaxCount.readFields(in);
		}

		@Override
		public String toString() {
			return "StationWritable [sumMinTemp=" + sumMinTemp
					+ ", sumMinCount=" + sumMinCount + ", sumMaxTemp="
					+ sumMaxTemp + ", sumMaxCount=" + sumMaxCount + "]";
		}

	}

	/**
	 * Mapper class that emits the station and Station iff the type is of TMIN or TMAX.
	 */
	public static class MapReduceWithCombinerMapper extends Mapper<Object, Text, Text, StationWritable>{
		
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] stationInfo = value.toString().split(",");
			StationWritable tempStn = new StationWritable();
			if(stationInfo[2].equals(MIN_TEMP)){
				tempStn.sumMinTemp = new Text(stationInfo[3]);
				tempStn.sumMinCount = new Text("1");
				context.write(new Text(stationInfo[0]), tempStn);
			}
			else if(stationInfo[2].equals(MAX_TEMP)){
				tempStn.sumMaxTemp = new Text(stationInfo[3]);
				tempStn.sumMaxCount = new Text("1");
				context.write(new Text(stationInfo[0]), tempStn);
			}
		}
	}
	/**
	 * Combiner class that aggregates TMIN and TMAX of a particular station
	 */
	public static class MapReduceCombiner extends Reducer<Text,StationWritable,Text,StationWritable>{
		public static int count ;
		
		@Override
		protected void setup(
				Reducer<Text, StationWritable, Text, StationWritable>.Context context)
				throws IOException, InterruptedException {
			count = 0;
		}
		
		@Override
		protected void reduce(Text key, Iterable<StationWritable> valueIter, Context context) throws IOException, InterruptedException {
			long minCount = 0;
			long maxCount = 0;
			double minSum = 0.0;
			double maxSum = 0.0;
			for (StationWritable stationWritable : valueIter) {
				if(!stationWritable.sumMaxTemp.toString().equals(NOT_DEFINED)){
					maxCount+=Long.valueOf(stationWritable.sumMaxCount.toString());
					maxSum+=Double.valueOf(stationWritable.sumMaxTemp.toString());
				}
				if(!stationWritable.sumMinTemp.toString().equals(NOT_DEFINED)){
					minCount+=Long.valueOf(stationWritable.sumMinCount.toString());
					minSum+=Double.valueOf(stationWritable.sumMinTemp.toString());
				}
			}
			StationWritable stnWritable = new StationWritable();
			if(maxCount!=0){
				stnWritable.sumMaxTemp = new Text(String.valueOf(maxSum));
				stnWritable.sumMaxCount = new Text(String.valueOf(maxCount));
			}if(minCount!=0){
				stnWritable.sumMinTemp = new Text(String.valueOf(minSum));
				stnWritable.sumMinCount = new Text(String.valueOf(minCount));
			}
			context.write(key, stnWritable);
			
			++count;
		}
		
		@Override
		protected void cleanup(
				Reducer<Text, StationWritable, Text, StationWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("TOTAL NUMBER OF TIMES THE COMBINER WAS CALLED: "+ count);
		}
	}
	/**
	 * Reducer class that aggregates TMIN and TMAX per station and computes respective average.
	 */
	public static class MapReduceWithCombinerReducer extends Reducer<Text,StationWritable,Text,NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<StationWritable> valueIter, Context context) throws IOException, InterruptedException {
			long minCount = 0;
			long maxCount = 0;
			double minSum = 0.0;
			double maxSum = 0.0;
			for (StationWritable stationWritable : valueIter) {
				if(!stationWritable.sumMaxTemp.toString().equals(NOT_DEFINED)){
					maxCount+=Long.valueOf(stationWritable.sumMaxCount.toString());;
					maxSum+=Double.valueOf(stationWritable.sumMaxTemp.toString());
				}
				if(!stationWritable.sumMinTemp.toString().equals(NOT_DEFINED)){
					minCount+=Long.valueOf(stationWritable.sumMinCount.toString());;
					minSum+=Double.valueOf(stationWritable.sumMinTemp.toString());
				}
			}
			StringBuilder minTempStr = new StringBuilder();
			StringBuilder maxTempStr = new StringBuilder();
			
			if(maxCount!=0){
				maxTempStr.append(maxSum/maxCount);
			}else{
				maxTempStr.append(NOT_DEFINED);
			}
			if(minCount!=0){
				minTempStr.append(minSum/minCount);
			}else{
				minTempStr.append(NOT_DEFINED);
			}
			context.write(new Text(key+", "+minTempStr+","+maxTempStr),NullWritable.get());
		}
	}

	/**
	 * Driver method
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length < 2){
			System.out.println("Invalid Format <Input-File> [<Input-File] <OutputFile>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "MapReduceCombiner");
		job.setJarByClass(MapReduceWithCombiner.class);

		job.setMapperClass(MapReduceWithCombinerMapper.class);
		job.setReducerClass(MapReduceWithCombinerReducer.class);

		job.setCombinerClass(MapReduceCombiner.class);

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
