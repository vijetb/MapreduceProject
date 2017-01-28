package com.vijet.mr.pagerank;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * Mapper class that receives the pagerank values from the file through distributed cache and performs
 * and emits each of the iterations.The M contains each of the outlinks, associated with that node. 
 * It gets the pagerank share from the R matrix that is loaded into the memory and distributes its share to each of the outlinks. 
 * It also emits the pagerank value of 0.0 for itself so that if the node is a deadNode it will get its default share and those nodeId are not lost *
 */
public class PageRank_M_MapperIteration extends Mapper<Object, Text, LongWritable, DoubleWritable>{
	public Map<Long,Double> ranksMapping = new HashMap<Long, Double>();
	public  Map<Long,Double> localMapping = new HashMap<Long, Double>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		int isVal = context.getConfiguration().getInt("isValue", 0);
		if(isVal==0){
			throw new RuntimeException("Is Value is 0");
		}
		if(isVal==1){
			FileSystem fs = FileSystem.getLocal(context.getConfiguration());
			Path[] localPaths = context.getLocalCacheFiles();
			for(Path path: localPaths){
				if(!path.toString().contains("crc")){
					System.out.println(path.toString());
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
					String line = null;
					while((line=reader.readLine())!=null){
						String[] data = line.split(",");
						ranksMapping.put(Long.parseLong(data[0].trim()), Double.parseDouble(data[1].trim()));
					}
					reader.close();
				}

			}
		}else{
			File[] f = new File("./cacheFile").listFiles();
			for (File file : f) {
				if(!file.getName().contains(".crc")){
					BufferedReader reader = new BufferedReader(new FileReader(file));
					String line = null;
					while((line=reader.readLine())!=null){
						String[] data = line.split(",");
						ranksMapping.put(Long.parseLong(data[0].trim()), Double.parseDouble(data[1].trim()));
					}
					reader.close();	
				}

			}
		}

	}

	@Override
	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
		Long i = Long.parseLong(value.toString().split(":")[0]);	// 2:3,4,5
		String ks[] = value.toString().split(":")[1].split(",");
		final double prShare = 1.0/ks.length;
		for(String k: ks){
			if(localMapping.containsKey(Long.parseLong(k))){
				localMapping.put(Long.parseLong(k), localMapping.get(Long.parseLong(k))+prShare*ranksMapping.get(i));
			}else{
				localMapping.put(Long.parseLong(k), prShare*ranksMapping.get(i));
			}
		}
		if(!localMapping.containsKey(i)){
			localMapping.put(i,0.0);
		}
	}

	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		for (Long id : localMapping.keySet()) {
			context.write(new LongWritable(id), new DoubleWritable(localMapping.get(id)));
		}
	}
}