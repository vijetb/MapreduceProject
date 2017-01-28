package com.vijet.mr.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Model class that is used to compute M*R. 
 */
public class MRKey implements Writable,WritableComparable<MRKey>{
	public Long key;
	public Long matrixType; 

	
	public MRKey() {
		// TODO Auto-generated constructor stub
	}
	
	public MRKey(Long key, Long matrixType) {
		this.key = key;
		this.matrixType = matrixType;
	}

	@Override
	public int compareTo(MRKey o) {
		if(this.key.compareTo(o.key) == 0){
			return this.matrixType.compareTo(o.matrixType);
		}
		return this.key.compareTo(o.key);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(key);
		out.writeLong(matrixType);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readLong();
		matrixType = in.readLong();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result
				+ ((matrixType == null) ? 0 : matrixType.hashCode());
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
		MRKey other = (MRKey) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (matrixType == null) {
			if (other.matrixType != null)
				return false;
		} else if (!matrixType.equals(other.matrixType))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MRKey [key=" + key + ", matrixType=" + matrixType + "]";
	}
}
