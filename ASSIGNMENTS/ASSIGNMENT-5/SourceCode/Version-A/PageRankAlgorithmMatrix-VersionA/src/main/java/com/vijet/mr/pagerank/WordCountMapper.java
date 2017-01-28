package com.vijet.mr.pagerank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * Mapper class that emits all the values associated with the link. Similar to wordcount design pattern.
 * Inmapper combining is used to collect all the values pertaining to a value.
 */
public class WordCountMapper extends Mapper<Object, Text, LongWritable, DoubleWritable>{
	public Map<String,Double> map;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		map = new HashMap<String,Double>();
	}

	@Override
	protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
		String[] data = value.toString().split(",");

		if(map.containsKey(data[0].trim())){
			map.put(data[0].trim(), map.get(data[0].trim())+Double.valueOf(data[1].trim()));
		}else{
			map.put(data[0].trim(), Double.valueOf(data[1].trim()));
		}
	}

	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		for(String key:map.keySet()){
			context.write(new LongWritable(Long.parseLong(key)), new DoubleWritable(map.get(key)));
		}
	}
}
