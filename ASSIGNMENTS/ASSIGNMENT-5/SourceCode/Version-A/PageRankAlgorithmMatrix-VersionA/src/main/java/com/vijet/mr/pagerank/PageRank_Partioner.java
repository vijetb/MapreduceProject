package com.vijet.mr.pagerank;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition the keys such that all the links go to the same reducer based on the hashcode of the link.
 */
public class PageRank_Partioner extends Partitioner<MRKey, MRValue>{

	@Override
	public int getPartition(MRKey key, MRValue value, int numPartitions) {
		return (key.key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
