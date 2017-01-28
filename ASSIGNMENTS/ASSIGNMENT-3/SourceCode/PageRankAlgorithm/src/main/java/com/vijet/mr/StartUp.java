package com.vijet.mr;

import java.io.IOException;

public class StartUp {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String path = args[args.length-1];
		long TOTAL_NO_OF_LINKS = PageRankPreprocessor.preprocess(args);
		PageRankAlgorithm.runPageRankAlgorithm(TOTAL_NO_OF_LINKS,path);
		PageRankTop100Records.getTop50Records(args[args.length-1]);
	}
}
