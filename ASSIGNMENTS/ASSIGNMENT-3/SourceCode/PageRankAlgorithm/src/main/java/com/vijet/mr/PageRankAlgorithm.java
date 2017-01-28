package com.vijet.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Page rank algorithm.
 */
public class PageRankAlgorithm {
	public static final String TOTAL_NO_OF_DOCUMENTS = "TOTAL_NO_OF_DOCUEMNTS";
	public static final String TOTAL_LOSS_OF_DANGLING_NODES = "TOTAL_LOSS_OF_DANGLING_NODES";
	public static final int MAX_NO_OF_ITERATIONS = 10;

	public enum COUNTERS{
		DANGLING_NODES_PR_KEY, TOTAL_NO_OF_LINKS
	}

	/**
	 * Bean class that is used as a Value for Mapper Output
	 */
	public static class PageRankLinkRecord implements Writable{

		public Text outlinks;
		public Text prScore; 

		public PageRankLinkRecord() {
			outlinks = new Text();
			prScore = new Text();
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			outlinks.write(out);
			prScore.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			outlinks.readFields(in);
			prScore.readFields(in);
		}
	}

	/**
	 * Mapper class that computes the pagerank share that should be distributed over each of the nodes 
	 * due to the dangling nodes to the page rank value of each of the record and share it equally accross
	 * all the outlinks. Also emit the outlinks associated with the record.
	 */
	public static class IterativePageRankMapper extends Mapper<Object,Text,Text,PageRankLinkRecord>{
		private static final double ALPHA = 0.15;
		private static double DANGLING_PAGE_RANK_CORRECTION = 0.0;
		private static final double CONVERSION_FACTOR = 1000000000000.0;
		private static Set<String> links;
		private static double TOTAL_PAGE_RANK_VALUE = 0.0;

		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			double totalCorrection = context.getConfiguration().getDouble(TOTAL_LOSS_OF_DANGLING_NODES, 0.0)/CONVERSION_FACTOR;
			long totalValues = context.getConfiguration().getLong(TOTAL_NO_OF_DOCUMENTS, 12500000L);
			DANGLING_PAGE_RANK_CORRECTION = (1-ALPHA) * totalCorrection/totalValues;
			links = new HashSet<String>();
			TOTAL_PAGE_RANK_VALUE = totalCorrection;
		}

		@Override
		protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
			String nodeId  = value.toString().split("\\|")[0];
			String adjList = value.toString().split("\\|")[1].split("~~")[0];
			String prValue = value.toString().split("\\|")[1].split("~~")[1];

			TOTAL_PAGE_RANK_VALUE+=Double.parseDouble(prValue);
			links.add(nodeId);

			String[] outlinks = null;
			// dangling node 
			if(adjList.equals("NA")){
				PageRankLinkRecord record = new PageRankLinkRecord();
				record.outlinks = new Text("#");
				record.prScore = new Text(String.valueOf(DANGLING_PAGE_RANK_CORRECTION));
				context.write(new Text(nodeId), record);
				return;
			}

			outlinks = adjList.split("~");

			double pageRankShare = (DANGLING_PAGE_RANK_CORRECTION + Double.valueOf(prValue))/outlinks.length;

			//output the AdjacencyList for the nodeId
			PageRankLinkRecord adjListrecord = new PageRankLinkRecord();
			adjListrecord.outlinks = new Text(adjList);
			adjListrecord.prScore = new Text(String.valueOf(Double.NEGATIVE_INFINITY));;
			context.write(new Text(nodeId), adjListrecord);
			// output the pagerank to each of the outlinks
			for(String link: outlinks){
				PageRankLinkRecord record = new PageRankLinkRecord();
				record.outlinks = new Text("#");
				record.prScore = new Text(String.valueOf(Double.valueOf(pageRankShare)));
				context.write(new Text(link), record);
			}
		}

		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException {
			context.getCounter(COUNTERS.TOTAL_NO_OF_LINKS).increment(links.size());
			System.out.println("**********************");
			System.out.println("TOTAL PAGE RANK: " + TOTAL_PAGE_RANK_VALUE);
			System.out.println("**********************");
		}
	}

	/**
	 * Reducer class that accumulates the pagerank of all the nodes and computes the new page rank. 
	 * If the node is a dangling node then it will update the counter that accumulates the pagerank
	 * of dangling nodes.
	 */
	public static class IterativePageRankReducer extends Reducer<Text,PageRankLinkRecord, NullWritable, Text>{
		private static final double CONVERSION_FACTOR = 1000000000000.0;
		private static  Long TOTAL_DOCUMENTS = 120000L;
		private static final double ALPHA = 0.15;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			TOTAL_DOCUMENTS = context.getConfiguration().getLong(TOTAL_NO_OF_DOCUMENTS, 12500000L);
		}

		@Override
		protected void reduce(Text nodeName, Iterable<PageRankLinkRecord> pageRankLinkRecordIter, Context context) throws IOException, InterruptedException {
			Iterator<PageRankLinkRecord> prRecordIter = pageRankLinkRecordIter.iterator();
			String outlinksList = null;
			double newPrScore = 0.0;
			
			while(prRecordIter.hasNext()){
				PageRankLinkRecord record = prRecordIter.next();

				if(outlinksList==null && Double.valueOf(record.prScore.toString())==Double.NEGATIVE_INFINITY){
					outlinksList = record.outlinks.toString();
					continue;
				}
				newPrScore+=Double.valueOf(record.prScore.toString());
			}

			newPrScore = 1.0/TOTAL_DOCUMENTS*ALPHA + (1.0-ALPHA) * newPrScore;

			if(outlinksList==null){
				context.getCounter(COUNTERS.DANGLING_NODES_PR_KEY).increment((long) (newPrScore * CONVERSION_FACTOR));
				outlinksList="NA";
			}

			context.write(NullWritable.get(), new Text(nodeName+"|"+outlinksList+"~~"+newPrScore));
		}
	}

	public static void runPageRankAlgorithm(long TotalNoOfLinks, String path) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set(TOTAL_NO_OF_DOCUMENTS, String.valueOf(TotalNoOfLinks));
		conf.set(TOTAL_LOSS_OF_DANGLING_NODES, "0.0");

		initializePageRank(conf,0,path);

		for(int i=1; i < MAX_NO_OF_ITERATIONS;i++){
			TempClass temp = iterate(conf,i,path);
			conf.set(TOTAL_LOSS_OF_DANGLING_NODES, String.valueOf(temp.pageRankValues));
			conf.set(TOTAL_NO_OF_DOCUMENTS, String.valueOf(temp.totalLinks));
		}
	}

	public static void initializePageRank(Configuration conf,int ii, String path) throws IOException, ClassNotFoundException, InterruptedException {


		Job job = Job.getInstance(conf, "PageRankIterativePreprocessingJob"+ii);
		job.setJarByClass(PageRankAlgorithm.class);

		job.setMapperClass(IterativePageRankPreprocessingMapper.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(path+"-"+ii));

		FileOutputFormat.setOutputPath(job,new Path(path+"-"+(ii+1)));

		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
	}

	public static TempClass iterate(Configuration conf,int ii, String path) throws IOException, ClassNotFoundException, InterruptedException {


		Job job = Job.getInstance(conf, "PageRankIterativeJob"+ii);
		job.setJarByClass(PageRankAlgorithm.class);

		job.setMapperClass(IterativePageRankMapper.class);
		job.setReducerClass(IterativePageRankReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageRankLinkRecord.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(path+"-"+ii));

		FileOutputFormat.setOutputPath(job,new Path(path+"-"+(ii+1)));
		job.waitForCompletion(true);

		long value = job.getCounters().findCounter(COUNTERS.DANGLING_NODES_PR_KEY).getValue();
		long totalLinks = job.getCounters().findCounter(COUNTERS.TOTAL_NO_OF_LINKS).getValue();
		System.out.println("*******************************");
		System.out.println("ITERATION: " + ii);
		System.out.println("TOTAL-LINKS: " + totalLinks);
		System.out.println("*******************************");
		TempClass tempClass = new TempClass();
		tempClass.pageRankValues = value;
		tempClass.totalLinks = totalLinks;
		return tempClass;
	}
	
	/**
	 * Bean class used to pass the values between the iterations
	 */
	static class TempClass{
		long pageRankValues;
		long totalLinks;
	}



}
