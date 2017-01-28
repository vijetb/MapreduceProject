package com.vijet.mr.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper that Emits the ((i,j,k),value) where refers to the i row of the first matrix, 
 * j refers to the columns of the second matrix and k is the rowId of the first matrix.It emits the 0 as i as the 
 * we are multiplying DXR and R has only one Column. i=0 as D has only one Row.
 */
public  class PageRank_R_Mapper extends Mapper<Object, Text, PageRankDanglingKey, DoubleWritable>{
	@Override
	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split(",");	// 2:0.5
		PageRankDanglingKey k = new PageRankDanglingKey(0L, 0L, Long.valueOf(data[0].trim()));
		context.write(k, new DoubleWritable(Double.valueOf(data[1].trim())));
	}
}
