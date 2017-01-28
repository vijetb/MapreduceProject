package com.vijet.mr.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Model class that acts as a Key in computing the sum of page rank scores for all the dangling nodes.
 */
public class PageRankDanglingKey implements Writable, WritableComparable<PageRankDanglingKey>{
	public Long i;
	public Long j;
	public Long k;

	public PageRankDanglingKey() {

	}

	public PageRankDanglingKey(long i, long j, long k) {
		this.i = new Long(i);
		this.j = new Long(j);
		this.k = new Long(k);
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(i);
		out.writeLong(j);
		out.writeLong(k);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		i = in.readLong();
		j = in.readLong();
		k = in.readLong();
	}

	@Override
	public int compareTo(PageRankDanglingKey o) {
		if(this.i.equals(o.i)){
			if(this.j.equals(o.j)){
				return this.k.compareTo(o.k);
			}else{
				return this.j.compareTo(o.j);
			}
		}
		return this.i.compareTo(o.i);
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((i == null) ? 0 : i.hashCode());
		result = prime * result + ((j == null) ? 0 : j.hashCode());
		result = prime * result + ((k == null) ? 0 : k.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PageRankDanglingKey other = (PageRankDanglingKey) obj;
		if (i == null) {
			if (other.i != null)
				return false;
		} else if (!i.equals(other.i))
			return false;
		if (j == null) {
			if (other.j != null)
				return false;
		} else if (!j.equals(other.j))
			return false;
		if (k == null) {
			if (other.k != null)
				return false;
		} else if (!k.equals(other.k))
			return false;
		return true;
	}


}
