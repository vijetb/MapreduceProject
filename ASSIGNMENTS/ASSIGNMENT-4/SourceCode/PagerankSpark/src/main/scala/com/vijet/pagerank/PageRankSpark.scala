package com.vijet.pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext

object PageRankSpark {

	def main(args: Array[String]): Unit = {
	    val DANGLING_NODES_KEY = "DANGLING_NODES"
  		//Initialize the SparkContext and set the configuration Parameters. local[*] specifies the
	    // context to use maximum number of cores available on the machine that is running the program
	    var conf = new SparkConf().setAppName("PageRankAlgorithm").setMaster("local[*]");
			val sc = new SparkContext(conf)
			
			/**
			 * Preprocessing Task. The map task fetches each line from the input file and sends it to 
			 * the Parser to parse it. The parser is going to return the line in the format
			 * (link|outlink~outlink) in case of non-dangling-node and (link|) in case of dangling
			 * node. On the result of this map RDD, filter is applied to remove those nodes which caused
			 * the exception during parsing or invalid nodes in the input.
			 */
			val links = sc.textFile(args(0)).map(rec => com.vijet.parser.Parser.process(rec))
					.filter(rec => (rec.trim.length >= 1))

			/**
			 * For each of the valid links obtained after pre-processing, split the string using
			 * map and check if the link is dangling. If the link is not a dangling node, then emit
			 * outlinks list as empty list otherwise split on it and return the outlinks as list.
			 */
					val allLinks = links.map { x => {
						val parts = x.split ( "\\|")
								if(parts.size == 1){
									(parts (0), List[String]())  
								}else{ 
									(parts (0), parts (1).split("~").toList)  
								} 
					}// This map function takes a link and returns a List(List(link,outlink)…)) for each of the outlinks associated with it 
					//along with the original link
			}.map(x => {    
				List(List((x._1,x._2)),x._2.map { y => (y,List[String]())})
			})
			// The below two flatMap functions flatten the list that is generated in upper step and return a List of(link, List of //outlinks). 
			//This is final links list that contains all the nodes given in the corpus. 
			.flatMap { z => z }
			.flatMap { p => p }
			//This reduceByKey combines all the outlinks associated with a particular link into a single list.
			.reduceByKey((a,b) => (a++b)).persist()
			
			//Total Number of links
			val TOTAL_LINKS_COUNT = allLinks.count()
			//Default Pagerank value
			val DEFAULT_PR_SHARE = 1.0/TOTAL_LINKS_COUNT
			/**
			 * Default PageRank: Initialize all the pageranks of all the links to the default value I,e 1/Total_#_of_links
			 */
			var pageRanks = allLinks.keys.map{ link => (link,DEFAULT_PR_SHARE)}
			/**
			 * Distributed graph RDD that contains link->([outlinks],pagerank). This RDD
			 * will be during each iteration to share there PR values with all the outlinks and
			 * also during computation of the PageRank associated with the dangling nodes.
			 */
			var nodes = allLinks.join(pageRanks);

			// Iterate 10 times
			for(iteration <- 1 to 10){
			  /**
			   * Iterate over the nodes and filter out the dangling nodes. Map over the RDD obtained to 
			   * accumulate the scores associated with all the dangling nodes and take the sum.
			   */                    
				val danglingNodesPR =   nodes.values.filter(x=>x._1.isEmpty).reduce((p,q) => (List[String](),(p._2+q._2)))._2
				
				/**
				 * The following code is getting the pagerank of the dangling nodes using Accumalator approach. 
				 * I tried both the approaches but the above apporach was giving better time than this one.
				 */
//				val danglingNodesPR = sc.accumulator(0.0, "PAGE_RANK_VALUE")
//								//   println("DanglingNodes value before: " + iteration+ ":" + danglingNodesPR.value)
//
//								val contributions = nodes.values.flatMap{
//								case (outlinks,score) => {
//									if(outlinks.isEmpty){
//										danglingNodesPR+=score;
//										List()
//									}else{
//										val PR_SHARE = score/outlinks.size
//												outlinks.map { outlink => (outlink, PR_SHARE)}
//									}
//								}
//						}
//
//						contributions.collect()
				
				/**
				 * Iterate over all the nodes and each of the page rank share to all the outlinks associated with 
				 * that link. If the node is a dangling node then just emit empty list. The nodes that don't have any
				 * inlinks get lost during this process.                        
				 */
				val contributions = nodes.values.flatMap{
						case (outlinks,score) => {
							if(outlinks.isEmpty){
								List()
							}else{
								val PR_SHARE = score/outlinks.size
										outlinks.map { outlink => (outlink, PR_SHARE)}
							}
						}
				}
				/**
				 * Compute the danglingscore that should be added to each of the links
				 * given in the network.
				 */
				val danglingCorrection = danglingNodesPR/TOTAL_LINKS_COUNT
				
				/**
				 * Accumulate the pagerank scores for a particular link and map it over to compute the new
				 * pagerank including the danglingCorrection corresponding to the link
				 */
				pageRanks = contributions.reduceByKey (_ + _).mapValues[Double] (p =>  
					0.15 * DEFAULT_PR_SHARE + 0.85 * (danglingCorrection + p))
								
				/**				
				 * Distribute the pagerank share to those nodes that don't have any inlinks.
				 * This can be achieved by performing the leftOuterJoin on allLinks and 
				 * updating the pagerank scores for those tuples whose pagerank is not defined.
				 * For the tuples whose pagerank is defined is already addressed in earlier step
				 */
				nodes = allLinks.leftOuterJoin(pageRanks).map(f => { 
				    if(f._2._2 == None){
				      (f._1, (f._2._1,0.15 * DEFAULT_PR_SHARE + 0.85 * danglingCorrection))
				    }else{
				      (f._1, (f._2._1,f._2._2.get))
				    }})
			}

			/**Sort the ranks RDD by descending order of the values and take the top 100 records. The Map tasks inverts the    
			 * key and values and top function takes the top 100 records with repartition factor as 1. The combined output is
			 * written to the file
			 */
  val top100Records = sc.parallelize(pageRanks.map(x => (x._2,x._1)).top(100),1).saveAsTextFile(args(1))			// stop the spring context
			sc.stop()
	}
}