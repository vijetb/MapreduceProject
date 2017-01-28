package com.vijet.mr.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.vijet.mr.PageRankAlgorithm.COUNTERS;
/**
 * Accumulate the dangling score coming from all the dangling nodes and increment the counter. If the values for a
 * particular node does not have two nodes then that node is not a dangling node so that node is ignored.
 */
public class PageRank_DR_Reducer extends Reducer<PageRankDanglingKey, DoubleWritable, NullWritable, NullWritable>{

	final double CONVERSION_FACTOR = Math.pow(10, 16);

	@Override
	protected void reduce(PageRankDanglingKey key,Iterable<DoubleWritable> value,Context context)throws IOException, InterruptedException {
		double TOTAL_PAGE_RANK_DANGLING_SCORE = 0.0;
		Iterator<DoubleWritable> iter = value.iterator();
		while(iter.hasNext()){
			double firstValue = iter.next().get();
			if(!iter.hasNext())
				return;
			double val = iter.next().get() * firstValue;
			TOTAL_PAGE_RANK_DANGLING_SCORE += val;
			context.getCounter(COUNTERS.DANGLING_NODES_PR_KEY).increment((long) (TOTAL_PAGE_RANK_DANGLING_SCORE * CONVERSION_FACTOR));

		}
	}

}