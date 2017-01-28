package com.vijet.mr;

import java.io.IOException;

public class StartUp {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String path = args[args.length-1];
		long TOTAL_LINKS = PageRankPreprocessor.preprocess(args);
		//long TOTAL_LINKS = 4;
		PageRankAlgorithm.runPageRankAlgorithm(path,TOTAL_LINKS);
		PageRankTop100Records.getTop50Records(path);
	}
}
