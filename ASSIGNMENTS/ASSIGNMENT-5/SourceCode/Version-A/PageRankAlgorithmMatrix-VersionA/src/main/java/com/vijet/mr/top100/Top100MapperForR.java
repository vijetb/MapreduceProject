package com.vijet.mr.top100;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


/**
 * Mapper function that emits the dummy key and value of pagerank as key and actual link as
 * value.
 */
public class Top100MapperForR extends Mapper<Object,Text,Text,MapperValue>{
	public Map<String,Double> map = new HashMap<String, Double>();
	
	@Override
	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split(",");
		map.put(data[0], Double.valueOf(data[1].trim()));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		List<Map.Entry<String, Double>> records = new ArrayList<>(map.entrySet());
        Collections.sort(records, new Comparator<Map.Entry<String, Double>>() {

            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        
        int max = (records.size()>100?100:records.size());
        
        for(int i=0;i<max;i++){
        	Map.Entry<String, Double> e = records.get(i);
        	context.write(new Text(e.getKey()), new MapperValue(String.valueOf(e.getValue()),true));
        }
        
	}
}
