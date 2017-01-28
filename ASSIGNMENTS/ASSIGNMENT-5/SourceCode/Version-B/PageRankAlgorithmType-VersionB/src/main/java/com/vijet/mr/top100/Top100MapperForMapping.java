package com.vijet.mr.top100;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Mapper function that emits the dummy key and value of pagerank as key and actual link as
 * value.
 */
public class Top100MapperForMapping extends Mapper<Object,Text,Text,MapperValue>{
	public Map<String,String> map = new HashMap<String, String>();
	
	@Override
	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split("\t");
		map.put(data[1],data[1].trim());
		context.write(new Text(data[1]), new MapperValue(data[0].trim(),false));
	}
}
