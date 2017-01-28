package com.vijet.mr;

import java.io.IOException;
import java.net.URISyntaxException;

public class StartUp {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		String path = args[args.length-1];
		long TOTAL_LINKS = PageRankPreprocessor.preprocess(args);
		//long TOTAL_LINKS = 18395;
		PageRankAlgorithm.runPageRankAlgorithm(path,TOTAL_LINKS);
		PageRankTop100Records.getTop50Records(path);
	}
}
