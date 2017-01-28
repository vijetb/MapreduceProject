package com.vijet.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.vijet.mr.top100.MapperValue;
import com.vijet.mr.top100.PageRank_top100_Partioner;
import com.vijet.mr.top100.Top100MapperForMapping;
import com.vijet.mr.top100.Top100MapperForR;
import com.vijet.mr.top100.Top100Reducer;

/**
 * Job driver class that prints top 100 records from the input records.
 */
public class PageRankTop100Records {
	
	//Driver function
	public static void getTop50Records(String path) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Top100Records");
		job.setJarByClass(PageRankTop100Records.class);
		MultipleInputs.addInputPath(job,new Path(path+"/iter-0/PageRank"),TextInputFormat.class,Top100MapperForR.class);
		MultipleInputs.addInputPath(job,new Path(path+"/preprocessing/Mapping-r-00000"),TextInputFormat.class,Top100MapperForMapping.class);

		job.setReducerClass(Top100Reducer.class);
		
		job.setPartitionerClass(PageRank_top100_Partioner.class);
		
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapperValue.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job,new Path(path+"/final-ranking/output-top-100"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
