package com.vijet.mr;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.vijet.mr.pagerank.PageRankDanglingKey;
import com.vijet.mr.pagerank.PageRank_DR_Reducer;
import com.vijet.mr.pagerank.PageRank_D_Mapper;
import com.vijet.mr.pagerank.PageRank_MR_Reducer;
import com.vijet.mr.pagerank.PageRank_M_MapperIteration;
import com.vijet.mr.pagerank.PageRank_R_Mapper;

/**
 * Page rank algorithm.
 */
public class PageRankAlgorithm {
	public enum COUNTERS{
		DANGLING_NODES_PR_KEY, TOTAL_NO_OF_LINKS
	}

	public static void runPageRankAlgorithm(String path, long TOTAL_LINKS) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException{
		for(int i = 0 ; i < 10;i++){
			double PRScore = danglingScoreSum(i,path);
			pageRankComputation(i,path,TOTAL_LINKS,PRScore);
			System.out.println("****************Iteration " + i + "Completed***********************");
		}
	}
	
	public static double danglingScoreSum(int i, String path) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "PageRankDanglingScore");
		job.setJarByClass(PageRankAlgorithm.class);

		MultipleInputs.addInputPath(job,new Path(path+"/preprocessing/D-r-00000"),TextInputFormat.class,PageRank_D_Mapper.class);
		if(i==0){
			System.out.println("under first");
			MultipleInputs.addInputPath(job,new Path(path+"/preprocessing/R-r-00000"),TextInputFormat.class,PageRank_R_Mapper.class);
		}else{//TODO
			System.out.println("under second");
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
	
	public static void pageRankComputation(int i, String path, long TOTAL_LINKS, double PRScore) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
		Configuration conf1 = new Configuration();
		conf1.setLong("TOTAL_RECORDS", TOTAL_LINKS);
		conf1.setDouble("PageRankDanglingScore", PRScore);
		if(i==0){
			conf1.setInt("isValue", 1);
		}else{
			conf1.setInt("isValue", 2);
		}
		Job job1 = Job.getInstance(conf1, "PageRankScore");
		job1.setJarByClass(PageRankAlgorithm.class);
		job1.setMapperClass(PageRank_M_MapperIteration.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setReducerClass(PageRank_MR_Reducer.class);

		FileInputFormat.addInputPath(job1, new Path(path+"/preprocessing/M-r-00000"));
		FileOutputFormat.setOutputPath(job1,new Path(path+"/iter-"+i+"/PageRank"));

		if(i==0){
			job1.addCacheFile(new Path(path+"/preprocessing/R-r-00000").toUri());
		}else{
			job1.addCacheArchive(new URI(path+"/iter-"+(i-1)+"/PageRank"+"#cacheFile"));
		}
		job1.waitForCompletion(true);
	}






}
