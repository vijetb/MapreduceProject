package com.vijet.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.vijet.mr.PageRankAlgorithm.COUNTERS;
import com.vijet.mr.preprocessing.PageRankPreprocessingReducer;
import com.vijet.mr.preprocessing.PageRankPreprocessorMapper;

/**
 * Preprocessor task that sets up the Mapper and preproces the results. It produces the Matrix M, Matrix D,
 * Matrix R and the Mapping file
 */
public class PageRankPreprocessor {
	/**
	 * Driver method for this Program
	 */
	public static long preprocess(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "PageRankPreprocessingJob");
		job.setJarByClass(PageRankPreprocessor.class);

		job.setMapperClass(PageRankPreprocessorMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PageRankPreprocessingReducer.class);

		for(int i = 0 ; i <= args.length-2;i++){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(args[args.length-1]+"/preprocessing"));

		MultipleOutputs.addNamedOutput(job, "M", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "D", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "R", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "Mapping", TextOutputFormat.class, Text.class, Text.class);

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);

		// get the counters and Return
		long TOTAL_COUNTS = job.getCounters().findCounter(COUNTERS.TOTAL_NO_OF_LINKS).getValue();

		return TOTAL_COUNTS;
	}
}
