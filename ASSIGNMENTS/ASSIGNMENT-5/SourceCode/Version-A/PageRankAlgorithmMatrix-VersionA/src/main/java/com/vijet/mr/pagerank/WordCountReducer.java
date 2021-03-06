package com.vijet.mr.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/**
 * WordCount Reducer that accumulates the values related to a nodeId.
 */
public class WordCountReducer extends Reducer<LongWritable, DoubleWritable, Text, NullWritable>{
	public static double PAGE_RANK_SHARE;
	public static long TOTAL_LINKS;

	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		PAGE_RANK_SHARE = context.getConfiguration().getDouble("PageRankDanglingScore", 0.0);
		TOTAL_LINKS = context.getConfiguration().getLong("TOTAL_RECORDS", 10000000L);
	}

	@Override
	protected void reduce(LongWritable key,Iterable<DoubleWritable> values,Context context)throws IOException, InterruptedException {
		double val = 0.0;

		for(DoubleWritable v:values){
			val+=v.get();
		}
		val = 0.15/TOTAL_LINKS + 0.85 * (PAGE_RANK_SHARE+val);
		context.write(new Text(key.toString()+","+val), NullWritable.get());
	}

}
