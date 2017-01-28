package com.vijet.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * StartUp class! Usage java StartUp <filePath>
 */
public class StartUp {

	public static void main(String[] args) throws IOException, InterruptedException {
		if(args.length!=1){
			System.out.println("Invalid argument, please provide the filepath");
			System.exit(1);
		}
		File INPUT_FILE = new File(args[0]);
//		File INPUT_FILE = new File("input/1912.csv");
		List<String> inputData = new ArrayList<String>();
		
		BufferedReader reader = new BufferedReader(new FileReader(INPUT_FILE));
		String line = null;
		while((line=reader.readLine())!= null){
			inputData.add(line.trim());
		}
		reader.close();
		System.out.println("*** DATA LOADED SUCCESSFULLY ****");
//		// Single Threaded No Locks
		SingleThreadTempByStnNoLock avgTempNoLock = new SingleThreadTempByStnNoLock(inputData);
		avgTempNoLock.computeAvgStnTemperature();
		avgTempNoLock.printTime();
		delay();
//		// Multi threaded without lock
		MultiThreadTempByStnNoLock multiTempNoLock = new MultiThreadTempByStnNoLock(inputData);
		multiTempNoLock.computeAvgStnTemperature();
		multiTempNoLock.printTime();
		 delay();
//		// Multi threaded Coarse Lock
		MultiThreadTempByStnCoarseLock multiTempCoarseLock = new MultiThreadTempByStnCoarseLock(inputData);
		multiTempCoarseLock.computeAvgStnTemperature();
		multiTempCoarseLock.printTime();
		 delay();
		// Multi threaded Fine lock
		MultiThreadTempByStnFineLock multiTempFineLock = new MultiThreadTempByStnFineLock(inputData);
		multiTempFineLock.computeAvgStnTemperature();
		multiTempFineLock.printTime();
		 delay();
//		// Multi threaded non shared Data structure
		MultiThreadTempByStnSeparateLock multiTempShared= new MultiThreadTempByStnSeparateLock(inputData);
		multiTempShared.computeAvgStnTemperature();
		multiTempShared.printTime();
	}

	private static void delay() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
