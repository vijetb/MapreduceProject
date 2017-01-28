package com.vijet.mr.pagerank;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This comparator is used for secondary design pattern such that the reducer always receives the R matrix 
 * values first then the M values. 
 */

public class PageRank_MR_GroupComparator extends WritableComparator{
	public PageRank_MR_GroupComparator() {
		super(MRKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MRKey k1 = (MRKey) a;
		MRKey k2 = (MRKey) b;
		
		return k1.key.compareTo(k2.key);
	}
}
