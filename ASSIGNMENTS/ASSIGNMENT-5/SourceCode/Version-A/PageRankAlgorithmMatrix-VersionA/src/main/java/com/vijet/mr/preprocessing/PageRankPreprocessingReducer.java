package com.vijet.mr.preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.vijet.mr.PageRankAlgorithm.COUNTERS;

/**
 * Reducer accumulates all the links in the graph. For each link reducer assigns an unique id to it. If the link  
 * is a dangling node then it will update that link Id in the dangling file with nodeId as name, 1/|V| as its value. 
 * If a link is a not a dangling node, then that link is written to M file in sparse format with linkId and outlinksId. 
 * It also assigns default pagerank value to all the nodes in the graph.
 */
public class PageRankPreprocessingReducer extends Reducer<Text,Text,Text,Text>{
	public static  Map<String,Long> linkMapping;
	public static Long count = 0L;
	public static MultipleOutputs<Text, Text> mos;
	public static Set<Long> danglingNodes;

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		linkMapping = new HashMap<String, Long>();
		danglingNodes = new HashSet<Long>();
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		// assigns unique id to all the links.  
		if(!linkMapping.containsKey(key.toString())){
			mos.write("Mapping", new Text(key.toString()), new Text(String.valueOf(count)));
			linkMapping.put(key.toString(), count);
			count++;
		}

		Iterator<Text> iter = value.iterator();
		List<Long> outlinksMapping = new ArrayList<Long>();
		while(iter.hasNext()){
			String s = iter.next().toString();
			if(s.trim().isEmpty()) continue;
			if(!linkMapping.containsKey(s.toString())){
				mos.write("Mapping", new Text(s.toString()), new Text(String.valueOf(count)));
				linkMapping.put(s.toString(), count);
				count++;
			}
			outlinksMapping.add(linkMapping.get(s));
		}
		// If a link is a dangling node then it updates the dangling set otherwise it writes the linkId with listOfOutlinksIds
		if(outlinksMapping.isEmpty()){ //write it to dangling nodes
			danglingNodes.add(linkMapping.get(key.toString()));
		}else{// write it to "M"
			StringBuilder builder = new StringBuilder();
			builder.append(linkMapping.get(key.toString())+":");
			for (Long outlinkId : outlinksMapping) {
				builder.append(outlinkId+",");
			}
			mos.write("M", NullWritable.get(), new Text(builder.toString().substring(0,builder.toString().length()-1)));
		}
	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
		//DanglingNodes
		final Long TOTAL_NODES = count;
		final double DEFAULT_VALUE = 1.0/TOTAL_NODES;
		for (Long danglingNodeId : danglingNodes) {
			mos.write("D", NullWritable.get(), new Text(danglingNodeId+":"+DEFAULT_VALUE));
		}
		for(long i = 0L; i < TOTAL_NODES; i++){
			mos.write("R", NullWritable.get(), new Text(i+","+DEFAULT_VALUE));
		}
		mos.close();
		// Update the counter
		context.getCounter(COUNTERS.TOTAL_NO_OF_LINKS).setValue(TOTAL_NODES);
	}
}
