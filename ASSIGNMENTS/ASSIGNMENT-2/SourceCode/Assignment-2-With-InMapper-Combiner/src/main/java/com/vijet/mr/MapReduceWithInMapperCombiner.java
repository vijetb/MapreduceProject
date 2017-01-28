package com.vijet.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
 * There is combiner in the program.
 */
public class MapReduceWithInMapperCombiner {

	public static final String MIN_TEMP = "TMIN";
	public static final String MAX_TEMP = "TMAX";
	public static final String NOT_DEFINED = "Not Defined";

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

	}
	/**
	 * Mapper class that performs inmapper combining and emits total minimum and maximum 
	 * temperature for a particular station. The maps are initialized in the setup() method.
	 * The maps are updated if the stationId is present in the map during each map call or else 
	 * they are inserted into the map. At last in cleanUp, the contents of the map are written out
	 * to the buffer.
	 */
	public static class MapReduceWithInMapperCombinerMapper extends Mapper<Object, Text, Text, StationWritable>{
		//Map to store the min Temp and max Temp for a particular station
		private static Map<String,Double> minTempMapping,maxTempMapping;
		//Map to store the minTempCount, maxTempCount
		private static Map<String,Long> minTempCount,maxTempCount;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, StationWritable>.Context context) throws IOException, InterruptedException {
			minTempMapping = new HashMap<String, Double>();
			maxTempMapping = new HashMap<String, Double>();
			minTempCount = new HashMap<String, Long>();
			maxTempCount= new HashMap<String, Long>();
		}
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] stationInfo = value.toString().split(",");
			
			if(stationInfo[2].trim().equals(MAX_TEMP)){
				if(maxTempMapping.containsKey(stationInfo[0].trim())){
					maxTempMapping.put(stationInfo[0].trim(), maxTempMapping.get(stationInfo[0].trim())+Double.valueOf(stationInfo[3].trim()));
					maxTempCount.put(stationInfo[0].trim(), maxTempCount.get(stationInfo[0].trim())+1);
				}else{
					maxTempMapping.put(stationInfo[0].trim(), Double.valueOf(stationInfo[3].trim()));
					maxTempCount.put(stationInfo[0].trim(),1L);
				}
			}
			if(stationInfo[2].trim().equals(MIN_TEMP)){
				if(minTempMapping.containsKey(stationInfo[0].trim())){
					minTempMapping.put(stationInfo[0].trim(), minTempMapping.get(stationInfo[0].trim())+Double.valueOf(stationInfo[3].trim()));
					minTempCount.put(stationInfo[0].trim(), minTempCount.get(stationInfo[0].trim())+1);
				}else{
					minTempMapping.put(stationInfo[0].trim(), Double.valueOf(stationInfo[3].trim()));
					minTempCount.put(stationInfo[0].trim(), 1L);
				}
			}
		} 
		
		@Override
		protected void cleanup(Mapper<Object, Text, Text, StationWritable>.Context context) throws IOException, InterruptedException {
			Set<String> allKeys = new HashSet<String>();
			allKeys.addAll(minTempMapping.keySet());
			allKeys.addAll(maxTempMapping.keySet());
			for (String stnId : allKeys) {
				StationWritable tempStation = new StationWritable();
				if(maxTempMapping.containsKey(stnId)){
					//tempStation.avgMaxTemp = new Text(String.valueOf(maxTempMapping.get(stnId)/maxTempCount.get(stnId)));
					tempStation.sumMaxTemp = new Text(String.valueOf(maxTempMapping.get(stnId)));
					tempStation.sumMaxCount = new Text(String.valueOf(maxTempCount.get(stnId)));
				}
				if(minTempMapping.containsKey(stnId)){
					tempStation.sumMinTemp = new Text(String.valueOf(minTempMapping.get(stnId)));
					tempStation.sumMinCount = new Text(String.valueOf(minTempCount.get(stnId)));
				}
				context.write(new Text(stnId), tempStation);
			}
		}
	}
	/**
	 * Reducer class that aggregates TMIN and TMAX per station and computes respective average.
	 */
	public static class MapReduceWithInMapperCombinerReducer extends Reducer<Text,StationWritable,Text,NullWritable>{
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

		Job job = Job.getInstance(conf, "MapReduceWithInMapperCombiner");
		job.setJarByClass(MapReduceWithInMapperCombiner.class);

		job.setMapperClass(MapReduceWithInMapperCombinerMapper.class);
		job.setReducerClass(MapReduceWithInMapperCombinerReducer.class);

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
