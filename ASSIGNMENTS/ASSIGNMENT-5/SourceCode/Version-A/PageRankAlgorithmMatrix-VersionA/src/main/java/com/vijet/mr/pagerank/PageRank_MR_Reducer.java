package com.vijet.mr.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * As secondary design pattern is used, the matrixIds are sorted and if the first record is not coming from R,
 * then that node is a dead node. So it writes that node to the intermediate node (indicating that node is a dead node). 
 * If not, then it gathers the page rank score and emits its value to that particular node.
 */
public class PageRank_MR_Reducer extends Reducer<MRKey, MRValue, Text, NullWritable>{

	public static double PAGE_RANK_SHARE;
	public static long TOTAL_LINKS;

	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		PAGE_RANK_SHARE = context.getConfiguration().getDouble("PageRankDanglingScore", 0.0);
		TOTAL_LINKS = context.getConfiguration().getLong("TOTAL_RECORDS", 10000000L);
	}

	@Override
	protected void reduce(MRKey key,Iterable<MRValue> value,Context context)throws IOException, InterruptedException {
		double score = 0.0;
		Iterator<MRValue> iter = value.iterator();
		MRValue val = iter.next();
		if(key.matrixType != 1L){
			score = (0.15/TOTAL_LINKS)+(0.85*PAGE_RANK_SHARE);
			context.write(new Text(String.valueOf(key.key)+",0.0"), NullWritable.get());
			context.write(new Text(val.rowOrColumn+","+val.value*score), NullWritable.get());
		}else{
			score = val.value;
		}
		while(iter.hasNext()){
			MRValue tempMValue = iter.next();
			context.write(new Text(tempMValue.rowOrColumn+","+(score*tempMValue.value)), NullWritable.get());
		}
	}

}