package com.vijet.mr.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Model class that acts as a value that emits the matrixtype, index, and value.
 */
public class MRValue implements Writable{

	public String matrix;
	public Long rowOrColumn;
	public Double value;

	public MRValue() {

	}

	public MRValue(String m, long j, Double k) {
		this.matrix = new String(m);
		this.rowOrColumn = new Long(j);
		this.value = new Double(k);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(matrix);
		out.writeLong(rowOrColumn);
		out.writeDouble(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		matrix = in.readUTF();
		rowOrColumn = in.readLong();
		value = in.readDouble();
	}

	@Override
	public String toString() {
		return "MRValue [matrix=" + matrix + ", rowOrColumn=" + rowOrColumn
				+ ", value=" + value + "]";
	}

	
}
