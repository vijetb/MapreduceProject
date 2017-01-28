package com.vijet.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job that prints top 100 records from the input records.
 */
public class PageRankTop100Records {

	/**
	 * Bean class that acts a key for mapper function. It implements Writable and the keys are
	 * always sorted by descending order of the values.
	 */
	public static class MapperKey implements Writable, WritableComparable<MapperKey>{

		private static String key = "DummyKey" ;
		public double value;
		
		@Override
		public int compareTo(MapperKey o) {
			return Double.compare(o.value, this.value);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(key);
			out.writeDouble(value);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			key = in.readUTF();
			value = in.readDouble();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			long temp;
			temp = Double.doubleToLongBits(value);
			result = prime * result + (int) (temp ^ (temp >>> 32));
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
			MapperKey other = (MapperKey) obj;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			if (Double.doubleToLongBits(value) != Double
					.doubleToLongBits(other.value))
				return false;
			return true;
		}
	}
	
	
	/**
	 * Mapper function that emits the dummy key and value of pagerank as key and actual link as
	 * value.
	 */
	public static class Top100Mapper extends Mapper<Object,Text,MapperKey,Text>{
		@Override
		protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
			double pageRankValue = Double.valueOf(value.toString().split("~~")[1]);
			MapperKey tempKey = new MapperKey();
			tempKey.value = pageRankValue;
			context.write(tempKey, value);
		}
	}
	
	/**
	 * Grouping comparator that groups all the records by key of the MapperKey Object
	 */	
	public static class IterativeComparator extends WritableComparator{
		public IterativeComparator() {
			super(MapperKey.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			MapperKey m1 = (MapperKey) a;
			MapperKey m2 = (MapperKey) b;
			return m1.key.compareTo(m2.key);
		}
	}
	
	/**
	 * Reducer that prints top 100 records for a particular key. As there is mapper emits only
	 * key, only one reduce call will be called and as grouping comparator is used, the values are
	 * sorted in descending order and top 100 records are fetched.
	 */
	public static class Top100Reducer extends Reducer<MapperKey,Text,NullWritable,Text>{
		public static final int TOP_100 = 100;
		@Override
		protected void reduce(MapperKey key, Iterable<Text> value,Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter= value.iterator();
			int count = 0;
			while(iter.hasNext()){
				if(count==100){
					break;
				}
				++count;
				Text t = iter.next();
				
				String nodeId = t.toString().split("\\|")[0];
				String adjList = t.toString().split("\\|")[1].split("~~")[0];
				String prValue = t.toString().split("\\|")[1].split("~~")[1];
				context.write(NullWritable.get(), new Text(nodeId+" "+prValue));
			}
		}
	}
	
	//Driver function
	public static void getTop50Records(String path) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Top100Records");
		job.setJarByClass(PageRankTop100Records.class);
		job.setMapperClass(Top100Mapper.class);
		job.setReducerClass(Top100Reducer.class);
		
		job.setGroupingComparatorClass(IterativeComparator.class);
		
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(MapperKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(path+"-10"));
		FileOutputFormat.setOutputPath(job,new Path(path+"output-top-100"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
