package com.vijet.mr.top100;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Reducer that prints top 100 records for a particular key. As there is mapper emits only
 * key, only one reduce call will be called and as grouping comparator is used, the values are
 * sorted in descending order and top 100 records are fetched.
 */
public class Top100Reducer extends Reducer<Text,MapperValue,NullWritable,Text>{
	public static final int TOP_100 = 100;
	public static Map<String,Double> rankMap = new HashMap<String, Double>();
	
	@Override
	protected void reduce(Text key, Iterable<MapperValue> value,Context context)
			throws IOException, InterruptedException {
		String first = null;
		Double d = null;
		Iterator<MapperValue> iter = value.iterator();
		while(iter.hasNext()){
			MapperValue mapValue = iter.next();
			if(mapValue.isRank){
				d = Double.valueOf(mapValue.key);
			}else{
				first = mapValue.key;
			}
		}
		if(d!=null){
			rankMap.put(first, d);
		}
	}
	
	@Override
	protected void cleanup(
			Reducer<Text, MapperValue, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		List<Map.Entry<String, Double>> records = new ArrayList<>(rankMap.entrySet());
        Collections.sort(records, new Comparator<Map.Entry<String, Double>>() {

            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        
        int max = (records.size()>100?100:records.size());
        
        for(int i=0;i<max;i++){
        	Map.Entry<String, Double> e = records.get(i);
        	context.write(NullWritable.get(), new Text(e.getKey()+"   "+e.getValue()));
        }
	}
}
