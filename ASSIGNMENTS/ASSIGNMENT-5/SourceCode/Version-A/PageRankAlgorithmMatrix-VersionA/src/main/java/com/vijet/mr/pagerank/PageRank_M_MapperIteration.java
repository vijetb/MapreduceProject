package com.vijet.mr.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Mapper class that emits the value with all the outlinks associated with the that nodeId.
 */
public class PageRank_M_MapperIteration extends Mapper<Object, Text, MRKey, MRValue>{
	@Override
	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
		String i = value.toString().split(":")[0];	// 2:3,4,5
		String ks[] = value.toString().split(":")[1].split(",");
		final double prShare = 1.0/ks.length;
		for(String k: ks){
			MRValue val = new MRValue("M",Long.valueOf(k),prShare);
			context.write(new MRKey(Long.valueOf(i),2L), val);
		}
	}
}
