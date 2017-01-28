package com.vijet.mr.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * Mapper class that emits all the ranks of all the nodes along with the matrix type
 */
public class PageRank_R_MapperIteration extends Mapper<Object, Text, MRKey, MRValue>{

	@Override
	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
		String k[] = value.toString().split(","); // 12,0.5
		MRValue val = new MRValue("R",0L,Double.valueOf(k[1]));
		context.write(new MRKey(Long.valueOf(k[0]),1L), val);
	}
}
