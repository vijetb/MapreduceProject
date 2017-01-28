package com.vijet.mr.top100;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partioner that takes makes all the local top k records move to only one reducer
 */
public class PageRank_top100_Partioner extends Partitioner<Text, MapperValue>{

	@Override
	public int getPartition(Text key, MapperValue value, int numPartitions) {
		return 0;
	}

}
