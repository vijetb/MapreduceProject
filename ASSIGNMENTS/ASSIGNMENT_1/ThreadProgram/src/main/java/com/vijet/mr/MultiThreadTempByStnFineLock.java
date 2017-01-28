package com.vijet.mr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Class that computes the Avg temperature of the station with FineLock on the datastructure shared
 * between the worker threads.
 */
public class MultiThreadTempByStnFineLock {
	private final static String TEMP_TYPE = "TMAX";
	/**
	 * Map that is shared between all the worker threads.
	 */
	private final  Map<String,Station> stationTempMapping = new HashMap<String,Station>();
	private final List<String> inputData;
	private final List<Long> avgTime = new ArrayList<Long>();

	public MultiThreadTempByStnFineLock(List<String> data){
		this.inputData = data;
	}

	/**
	 * Computes the Avg temperature of all the station
	 */
	public void computeAvgStnTemperature(){
		try {
			for(int i = 0 ; i < 10; i++){
				computeTemperature();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Computes the Avg temperature of the stations by dividing the inputs across the worker thread.
	 */
	public void computeTemperature() throws InterruptedException{
		stationTempMapping.clear();
		long startTime = System.currentTimeMillis();

		int DOC_COUNT = inputData.size();

		Thread t1 = new Thread(new WorkerThread(stationTempMapping, inputData.subList(0, DOC_COUNT/8)));
		Thread t2 = new Thread(new WorkerThread(stationTempMapping, inputData.subList(DOC_COUNT/8, 2*DOC_COUNT/8)));
		Thread t3 = new Thread(new WorkerThread(stationTempMapping, inputData.subList(2*DOC_COUNT/8, 3*DOC_COUNT/8)));
		Thread t4 = new Thread(new WorkerThread(stationTempMapping, inputData.subList(3*DOC_COUNT/8, 4*DOC_COUNT/8)));
		Thread t5 = new Thread(new WorkerThread(stationTempMapping, inputData.subList(4*DOC_COUNT/8, 5*DOC_COUNT/8)));
		Thread t6 = new Thread(new WorkerThread(stationTempMapping, inputData.subList(5*DOC_COUNT/8, 6*DOC_COUNT/8)));
		Thread t7 = new Thread(new WorkerThread(stationTempMapping, inputData.subList(6*DOC_COUNT/8, 7*DOC_COUNT/8)));
		Thread t8 = new Thread(new WorkerThread(stationTempMapping, inputData.subList(7*DOC_COUNT/8, DOC_COUNT)));

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t5.start();
		t6.start();
		t7.start();	
		t8.start();

		t1.join();
		t2.join();
		t3.join();
		t4.join();
		t5.join();
		t6.join();
		t7.join();
		t8.join();

		for (String stationId : stationTempMapping.keySet()) {
			Station station = stationTempMapping.get(stationId);
			station.computeAvg();
		}
		long endTime = System.currentTimeMillis();
		avgTime.add(endTime-startTime);
		//		System.out.println("START-TIME:"+ startTime);
		//		System.out.println("END-TIME:"+ endTime);
		//		System.out.println("TOTAL-TIME:"+ (endTime-startTime));
		//printToFile();
	}
	/**
	 * Prints the results to file. Used for debugging purpose
	 */
	private void printToFile() {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter("MultiThreadTempByStationFineLock.txt"));
			for (String stationId : stationTempMapping.keySet()) {
				writer.write(stationId +" "+stationTempMapping.get(stationId).getAvgTemp()+System.lineSeparator());
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints the output to the console
	 */
	public void printTime(){
		Long smallestTime = Long.MAX_VALUE;
		Long longestTime = 0L;
		Long sumTime = 0L;
		for (Long time : avgTime) {
			sumTime+=time;
			if(time<smallestTime){
				smallestTime = time;
			}
			if(time>longestTime){
				longestTime = time;
			}
		}
		System.out.println("**** MULTITHREADED AVG TEMPERATURE BY STATION FINE LOCK ********");
		System.out.println("AVG-TIME     : "+sumTime/10.0+" ms");
		System.out.println("SMALLEST-TIME: "+smallestTime+" ms");
		System.out.println("LONGEST-TIME : "+longestTime+" ms");
	}

	/**
	 * Worker thread that has a shared data structure that will be locked and updated
	 * for each station value.
	 */
	class WorkerThread implements Runnable{
		private Map<String,Station> sharedTempMapping;
		private List<String> data;

		public WorkerThread(Map<String,Station> stationTempMapping, List<String> data){
			this.sharedTempMapping = stationTempMapping;
			this.data = data;
		}

		public void run() {
			for (String data : data) {
				String[] dataValues = data.split(",");
				if(dataValues[2].equals(TEMP_TYPE)){
					if(sharedTempMapping.containsKey(dataValues[0].trim())){
						Station station = sharedTempMapping.get(dataValues[0].trim());
						synchronized (station) {
							station.appendTemp(Integer.valueOf(dataValues[3].trim()));
							station.appendCount();
							sharedTempMapping.put(dataValues[0].trim(), station);
							FibonaciUtil.fib(17);
						}
					}else{
						synchronized (sharedTempMapping) {
							sharedTempMapping.put(dataValues[0].trim(), new Station(Integer.valueOf(dataValues[3].trim()),1));	
						}
					}
				}
			}
		}

	}
} 

