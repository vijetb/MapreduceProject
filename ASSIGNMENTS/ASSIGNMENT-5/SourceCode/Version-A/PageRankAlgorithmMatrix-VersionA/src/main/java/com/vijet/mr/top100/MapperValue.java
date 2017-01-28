package com.vijet.mr.top100;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MapperValue implements Writable{

	public String key = "R" ;
	public Boolean isRank = false;
	
	public MapperValue() {
		// TODO Auto-generated constructor stub
	}
	
	public MapperValue(String key, Boolean isRank) {
		super();
		this.key = key;
		this.isRank = isRank;
	}



	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);
		out.writeBoolean(isRank);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readUTF();
		isRank = in.readBoolean();
	}

}