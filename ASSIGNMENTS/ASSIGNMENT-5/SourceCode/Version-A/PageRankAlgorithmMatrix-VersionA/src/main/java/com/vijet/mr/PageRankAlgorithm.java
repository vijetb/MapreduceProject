package com.vijet.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.vijet.mr.pagerank.MRKey;
import com.vijet.mr.pagerank.MRValue;
import com.vijet.mr.pagerank.PageRankDanglingKey;
import com.vijet.mr.pagerank.PageRank_DR_Reducer;
import com.vijet.mr.pagerank.PageRank_D_Mapper;
import com.vijet.mr.pagerank.PageRank_MR_GroupComparator;
import com.vijet.mr.pagerank.PageRank_MR_Reducer;
import com.vijet.mr.pagerank.PageRank_M_MapperIteration;
import com.vijet.mr.pagerank.PageRank_Partioner;
import com.vijet.mr.pagerank.PageRank_R_Mapper;
import com.vijet.mr.pagerank.PageRank_R_MapperIteration;
import com.vijet.mr.pagerank.WordCountMapper;
import com.vijet.mr.pagerank.WordCountReducer;

/**
 * Page rank algorithm Driver class.
 */
public class PageRankAlgorithm {
	public enum COUNTERS{
		DANGLING_NODES_PR_KEY, TOTAL_NO_OF_LINKS
	}

	public static void runPageRankAlgorithm(String path, long TOTAL_LINKS) throws ClassNotFoundException, IOException, InterruptedException {
		for(int i = 0 ; i < 10;i++){
			double PRScore = danglingValueComputation(i,path);
			pageRankComputation(i,path,TOTAL_LINKS,PRScore);
			System.out.println("****************Iteration " + i + "Completed***********************");
		}
	}
	
	public static double danglingValueComputation(int i, String path) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "PageRankDanglingScore");
		job.setJarByClass(PageRankAlgorithm.class);

		MultipleInputs.addInputPath(job,new Path(path+"/preprocessing/D-r-00000"),TextInputFormat.class,PageRank_D_Mapper.class);
		if(i==0){
			MultipleInputs.addInputPath(job,new Path(path+"/preprocessing/R-r-00000"),TextInputFormat.class,PageRank_R_Mapper.class);
		}else{//TODO
			MultipleInputs.addInputPath(job,new Path(path+"/iter-"+(i-1)+"/PageRank"),TextInputFormat.class,PageRank_R_Mapper.class);
		}

		job.setMapOutputKeyClass(PageRankDanglingKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(PageRank_DR_Reducer.class);

		FileOutputFormat.setOutputPath(job,new Path(path+"/iter-"+i+"/DanglingScore"));
		job.waitForCompletion(true);
		final double CONVERSION_FACTOR = Math.pow(10, 16);
		double PRScore = job.getCounters().findCounter(COUNTERS.DANGLING_NODES_PR_KEY).getValue()/CONVERSION_FACTOR;
		System.out.println("DanglingScore: " + PRScore);
		return PRScore;
	}
	
	public static void pageRankComputation(int i, String path,long TOTAL_LINKS,double PRScore) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf1 = new Configuration();
		conf1.setLong("TOTAL_RECORDS", TOTAL_LINKS);
		conf1.setDouble("PageRankDanglingScore", PRScore);

		Job job1 = Job.getInstance(conf1, "MR Computation");
		job1.setJarByClass(PageRankAlgorithm.class);

		MultipleInputs.addInputPath(job1,new Path(path+"/preprocessing/M-r-00000"),TextInputFormat.class,PageRank_M_MapperIteration.class);

		if(i==0){
			MultipleInputs.addInputPath(job1,new Path(path+"/preprocessing/R-r-00000"),TextInputFormat.class,PageRank_R_MapperIteration.class);
		}else{//TODO
			MultipleInputs.addInputPath(job1,new Path(path+"/iter-"+(i-1)+"/PageRank"),TextInputFormat.class,PageRank_R_MapperIteration.class);
		}
		
		job1.setMapOutputKeyClass(MRKey.class);
		job1.setMapOutputValueClass(MRValue.class);
		job1.setGroupingComparatorClass(PageRank_MR_GroupComparator.class);
		job1.setPartitionerClass(PageRank_Partioner.class);
		job1.setReducerClass(PageRank_MR_Reducer.class);

		FileOutputFormat.setOutputPath(job1,new Path(path+"/iter-"+i+"/Intermediate"));

		job1.waitForCompletion(true);
		//----------------------------------------------------------------------------------------------------------------------
		//MR-AGGREGATION
		Job job2 = Job.getInstance(conf1, "MR Aggregation");
		job2.setJarByClass(PageRankAlgorithm.class);

		MultipleInputs.addInputPath(job2,new Path(path+"/iter-"+i+"/Intermediate"),TextInputFormat.class,WordCountMapper.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setReducerClass(WordCountReducer.class);
		FileOutputFormat.setOutputPath(job2,new Path(path+"/iter-"+i+"/PageRank"));
		job2.waitForCompletion(true);

		System.out.println("****************Iteration " + i + "Completed***********************");
	}

}
