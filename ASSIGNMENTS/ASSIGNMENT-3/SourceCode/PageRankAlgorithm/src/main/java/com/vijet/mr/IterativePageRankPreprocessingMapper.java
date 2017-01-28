package com.vijet.mr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class that reshapes each of record with the DEFAULT_PAGE_RANK_VALUE.
 */
public class IterativePageRankPreprocessingMapper extends Mapper<Object,Text,NullWritable,Text>{
	public static final String TOTAL_NO_OF_DOCUMENTS = "TOTAL_NO_OF_DOCUEMNTS";
	public static double INITIAL_PAGE_RANK_VALUE = 0.0;
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		INITIAL_PAGE_RANK_VALUE = 1.0/context.getConfiguration().getLong(TOTAL_NO_OF_DOCUMENTS, 12500000L);
	}
	
	@Override
	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
		String nodeId = value.toString().split("\\|")[0];
		String adjList = value.toString().split("\\|")[1].split("~~")[0];
		context.write(NullWritable.get(), new Text(nodeId+"|"+adjList+"~~"+INITIAL_PAGE_RANK_VALUE));
	}
}