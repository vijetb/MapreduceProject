package com.vijet.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Class that reads the list of input files containing station ids and 
 * computes mean minimum temperature and mean maximum temperature, by station, by year. The program
 * first sorts with StationId,Year and then its groups with StationId.
 */
public class MapReduceSecondarySort {
	/**
	 * Constants
	 */
	public static final String MIN_TEMP = "TMIN";
	public static final String MAX_TEMP = "TMAX";
	public static final String NOT_DEFINED = "Not Defined";

	/**
	 * Bean class that acts as a Station Key containing StationId and Year. (Used for MapperOutputKey)
	 */
	public static class StationKeyWritable implements Writable,WritableComparable<StationKeyWritable>{
		//StationId
		public Text stationId;
		//Year
		public Text year;
		
		public StationKeyWritable() {
			stationId = new Text(NOT_DEFINED);
			year = new Text(NOT_DEFINED);
		}	
		
		@Override
		public void write(DataOutput out) throws IOException {
			stationId.write(out);
			year.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			stationId.readFields(in);
			year.readFields(in);
		}
		
		@Override
		public int compareTo(StationKeyWritable o) {
			if(this.stationId.toString().equals(o.stationId.toString())){
				return this.year.toString().compareTo(o.year.toString());
			}
			return this.stationId.toString().compareTo(o.stationId.toString());
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((stationId == null) ? 0 : stationId.hashCode());
			result = prime * result + ((year == null) ? 0 : year.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StationKeyWritable other = (StationKeyWritable) obj;
			if (stationId == null) {
				if (other.stationId != null)
					return false;
			} else if (!stationId.equals(other.stationId))
				return false;
			if (year == null) {
				if (other.year != null)
					return false;
			} else if (!year.equals(other.year))
				return false;
			return true;
		}
		
		
	}
	/**
	 * Bean class that acts as a Station Value containing avgMinTemp and avgMaxTemp.(Used for MapperOutputValue)
	 */
	public static class StationValueWritable implements Writable{
		// avgMin Temperature
		public Text avgMinTemp;
		// avgMax Temperature
		public Text avgMaxTemp;
		public Text totalMaxTempCount;
		public Text totalMinTempCount;

		public StationValueWritable() {
			avgMinTemp = new Text(NOT_DEFINED);
			avgMaxTemp = new Text(NOT_DEFINED);
			totalMaxTempCount = new Text(NOT_DEFINED);
			totalMinTempCount = new Text(NOT_DEFINED);
		}		

		@Override
		public void write(DataOutput out) throws IOException {
			avgMinTemp.write(out);
			avgMaxTemp.write(out);
			totalMaxTempCount.write(out);
			totalMinTempCount.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			avgMinTemp.readFields(in);
			avgMaxTemp.readFields(in);
			totalMaxTempCount.readFields(in);
			totalMinTempCount.readFields(in);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((avgMaxTemp == null) ? 0 : avgMaxTemp.hashCode());
			result = prime * result
					+ ((avgMinTemp == null) ? 0 : avgMinTemp.hashCode());
			result = prime
					* result
					+ ((totalMaxTempCount == null) ? 0 : totalMaxTempCount
							.hashCode());
			result = prime
					* result
					+ ((totalMinTempCount == null) ? 0 : totalMinTempCount
							.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StationValueWritable other = (StationValueWritable) obj;
			if (avgMaxTemp == null) {
				if (other.avgMaxTemp != null)
					return false;
			} else if (!avgMaxTemp.equals(other.avgMaxTemp))
				return false;
			if (avgMinTemp == null) {
				if (other.avgMinTemp != null)
					return false;
			} else if (!avgMinTemp.equals(other.avgMinTemp))
				return false;
			if (totalMaxTempCount == null) {
				if (other.totalMaxTempCount != null)
					return false;
			} else if (!totalMaxTempCount.equals(other.totalMaxTempCount))
				return false;
			if (totalMinTempCount == null) {
				if (other.totalMinTempCount != null)
					return false;
			} else if (!totalMinTempCount.equals(other.totalMinTempCount))
				return false;
			return true;
		}
	}
	
	/**
	 * Bean class used to store intermediate values for the map (Inmapper combining)
	 */
	public static class DummyStation{
		public Double maxTempSum;
		public Double minTempSum;
		public long maxTempCount =0L;
		public long minTempCount= 0L;
	}

	/**
	 * 	Mapper class that reads each records and writes it iff the record type is "TMAX" or "TMIN"
	 *  with (stationId,year) as key and (minTemp or maxTemp) as value. If the record reports TMAX then
	 *  TMIN value of the StationValueWritable is set as "NOT DEFINED" and vice versa.
	 */
	public static class MapReduceSecorndarySortMapper extends Mapper<Object, Text, StationKeyWritable, StationValueWritable>{
		private static Map<StationKeyWritable,DummyStation> map;
		
		@Override
		protected void setup(
				Mapper<Object, Text, StationKeyWritable, StationValueWritable>.Context context)
				throws IOException, InterruptedException {
			map = new HashMap<StationKeyWritable, DummyStation>();
		}
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] stationInfo = value.toString().split(",");
			
			StationKeyWritable stnKey = new StationKeyWritable();
			stnKey.stationId = new Text(stationInfo[0].trim());
			stnKey.year = new Text(stationInfo[1].substring(0, 4));
			
			DummyStation stationValue = new DummyStation();
			if(stationInfo[2].equals(MIN_TEMP)){
				stationValue.minTempSum= Double.valueOf(stationInfo[3]);
				stationValue.minTempCount = 1L;
				
				if(map.containsKey(stnKey)){
					DummyStation tempStation = map.get(stnKey);
					if(tempStation.minTempSum!=null){
						tempStation.minTempSum += stationValue.minTempSum;
						tempStation.minTempCount += stationValue.minTempCount;
					}else{
						tempStation.minTempSum = stationValue.minTempSum;
						tempStation.minTempCount = stationValue.minTempCount;
					}
					map.put(stnKey, tempStation);
				}else{
					map.put(stnKey, stationValue);
				}
			}
			else if(stationInfo[2].equals(MAX_TEMP)){
				stationValue.maxTempSum= Double.valueOf(stationInfo[3]);
				stationValue.maxTempCount = 1L;
				
				if(map.containsKey(stnKey)){
					DummyStation tempStation = map.get(stnKey);
					if(tempStation.maxTempSum!=null){
						tempStation.maxTempSum += stationValue.maxTempSum;
						tempStation.maxTempCount += stationValue.maxTempCount;
					}else{
						tempStation.maxTempSum = stationValue.maxTempSum;
						tempStation.maxTempCount = stationValue.maxTempCount;
					}
					map.put(stnKey, tempStation);
				}else{
					map.put(stnKey, stationValue);
				}
			}else{
				return;
			}
		}
		
		@Override
		protected void cleanup(
				Mapper<Object, Text, StationKeyWritable, StationValueWritable>.Context context)
				throws IOException, InterruptedException {
			
			for (Map.Entry<StationKeyWritable, DummyStation> stationEntry : map.entrySet()) {
				StationKeyWritable stnKey = stationEntry.getKey();
				DummyStation stnValue = stationEntry.getValue();
				
				StationValueWritable stnValueWritable = new StationValueWritable();
				if(stnValue.maxTempCount!=0){
					stnValueWritable.avgMaxTemp = new Text(String.valueOf(stnValue.maxTempSum));
					stnValueWritable.totalMaxTempCount = new Text(String.valueOf(stnValue.maxTempCount));
				}
				if(stnValue.minTempCount!=0){
					stnValueWritable.avgMinTemp = new Text(String.valueOf(stnValue.minTempSum));
					stnValueWritable.totalMinTempCount = new Text(String.valueOf(stnValue.minTempCount));
				}
				context.write(stnKey, stnValueWritable);
			}
		}

	}

	/**
	 * Reducer class that receives the records that are sorted by (stationId,Year) and grouped by (StationId). For each StationId there will be one
	 * reduce call and under each reduce call all the values for that particular station ids sorted by year are processed(Secondary Sort). During the
	 * iteration sum up the TMAX and TMIN of each for each year & when the year changes, compute the average and the output is written.
	 * 
	 * EXPLANATION of SECONDARY SORT:
	 * The mapper is going to emit the records with {stationId, year} as the key. The key comparator then sorts the keys
	 *  according to {stationId,year} (both in ascending order). The Partitioner partitioner the records based on the hashcode 
	 *  of the stationId so that all the records related to the particular station goes to the single reducer. 
	 *  On the reducer side, the grouping comparator groups the records by stationId. I.e. {s1,y1},{s1,y2},{s1,y3}.. 
	 *  are grouped together and are given as single reduce call for s1. y1,y2,y3 will also be sorted in ascending order as 
	 *  per the key comparator logic. Thus, for each reduce call all the records corresponding to the particular 
	 *  station will be processed according to the increasing order of year.
	 */
	public static class MapReduceSecorndarySortReducer extends Reducer<StationKeyWritable,StationValueWritable,Text,NullWritable>{
		@Override
		protected void reduce(StationKeyWritable key, Iterable<StationValueWritable> valueIter, Context context) throws IOException, InterruptedException {
			
			String year = "Dummy";
			double minSum = 0.0;
			Long minCount = 0L;
			double maxSum = 0.0;
			Long maxCount = 0L;
			StringBuilder temp = new StringBuilder();
			
			Iterator<StationValueWritable> iter = valueIter.iterator();
			while(iter.hasNext()){
				StationValueWritable stationWritable = iter.next();
				if(year.equals("Dummy")){
					temp.append("[");
					year = key.year.toString();
				}
				
				if(!year.equals(key.year.toString())){
					temp.append("("+year+","+(minCount==0?NOT_DEFINED:(minSum/minCount))+","+(maxCount==0?NOT_DEFINED:(maxSum/maxCount))+"),");
					year = key.year.toString();
					minSum = 0.0;minCount = 0L;
					maxSum = 0.0;maxCount = 0L;
				}
				
				if(!stationWritable.avgMaxTemp.toString().equals(NOT_DEFINED)){
					maxCount+=Long.valueOf(stationWritable.totalMaxTempCount.toString());
					maxSum+=Double.valueOf(stationWritable.avgMaxTemp.toString());
				}
				if(!stationWritable.avgMinTemp.toString().equals(NOT_DEFINED)){
					minCount+=Long.valueOf(stationWritable.totalMinTempCount.toString());
					minSum+=Double.valueOf(stationWritable.avgMinTemp.toString());
				}
			}
			temp.append("("+year+","+(minCount==0?NOT_DEFINED:(minSum/minCount))+","+(maxCount==0?NOT_DEFINED:(maxSum/maxCount))+"),");
			temp.deleteCharAt(temp.length()-1);
			temp.append("]");
			context.write(new Text(key.stationId+", "+temp.toString()), NullWritable.get());
		}
	}
	
	/**
	 * GroupingCompartor class that groups by stationId. 
	 */
	public static class MapReduceSecorndarySortGroupingComparator extends WritableComparator{
		
		public MapReduceSecorndarySortGroupingComparator(){
			super(StationKeyWritable.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			StationKeyWritable s1 = (StationKeyWritable) a;
			StationKeyWritable s2 = (StationKeyWritable) b;
			return s1.stationId.toString().compareTo(s2.stationId.toString());
		}
	}

	/**
	 * Partitioner class that partions the records based on the hashcode of the stationId so that all the records 
	 * Corresponding the same station goes to the same reducer.
	 */
	public static class SecondarySortPartioner extends Partitioner<StationKeyWritable, StationValueWritable>{

		@Override
		public int getPartition(StationKeyWritable key,
				StationValueWritable value, int numPartitions) {
			return (key.stationId.hashCode()& Integer.MAX_VALUE) % numPartitions;
		}
		
	}
	
	//Driver program
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
				if(args.length<2){
					System.out.println("Input format <inputFile> [inputFile] <output-file>");
					System.exit(1);
				}
				Configuration conf = new Configuration();
		
				Job job = Job.getInstance(conf, "MapReduceSecondarySortSample");
				job.setJarByClass(MapReduceSecondarySort.class);
		
				job.setMapperClass(MapReduceSecorndarySortMapper.class);
				job.setReducerClass(MapReduceSecorndarySortReducer.class);
		
				job.setGroupingComparatorClass(MapReduceSecorndarySortGroupingComparator.class);
				job.setPartitionerClass(SecondarySortPartioner.class);
				
				job.setMapOutputKeyClass(StationKeyWritable.class);
				job.setMapOutputValueClass(StationValueWritable.class);
		
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
				for(int i=0;i<=args.length-2;i++){
					FileInputFormat.addInputPath(job, new Path(args[i]));
				}
				FileOutputFormat.setOutputPath(job,new Path(args[args.length-1]));
				job.waitForCompletion(true);

	}
}

