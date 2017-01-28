package com.vijet.mr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Class that computes the Avg temperature of the stations, with each of the worker thread having the
 * maintaining the local copy of the map. The main thread later consolidates the results from all the 
 * worker thread and gives the result. 
 */
public class MultiThreadTempByStnSeparateLock {
	private final static String TEMP_TYPE = "TMAX";
	private final Map<String,Station> stationTempMappingThread1 = new HashMap<String,Station>();
	private final Map<String,Station> stationTempMappingThread2 = new HashMap<String,Station>();
	private final Map<String,Station> stationTempMappingThread3 = new HashMap<String,Station>();
	private final Map<String,Station> stationTempMappingThread4 = new HashMap<String,Station>();
	private final Map<String,Station> stationTempMappingThread5 = new HashMap<String,Station>();
	private final Map<String,Station> stationTempMappingThread6 = new HashMap<String,Station>();
	private final Map<String,Station> stationTempMappingThread7 = new HashMap<String,Station>();
	private final Map<String,Station> stationTempMappingThread8 = new HashMap<String,Station>();

	final List<String> inputData;
	List<Long> avgTime = new ArrayList<Long>();

	/**
	 * Computes the Avg temperature of all the station
	 */
	public MultiThreadTempByStnSeparateLock(List<String> data){
		this.inputData = data;
	}
	/**
	 * Computes the Avg temperature of the stations by dividing the inputs across the worker thread.
	 */
	public void computeAvgStnTemperature() throws InterruptedException{
		for(int i = 0 ; i < 10; i++){
			computeTemperature();
		}
	}

	public void computeTemperature() throws InterruptedException{
		stationTempMappingThread1.clear();stationTempMappingThread2.clear();
		stationTempMappingThread3.clear();stationTempMappingThread4.clear();
		stationTempMappingThread5.clear();stationTempMappingThread6.clear();
		stationTempMappingThread7.clear();stationTempMappingThread8.clear();
		long startTime = System.currentTimeMillis();

		int DOC_COUNT = inputData.size();

		Thread t1 = new Thread(new WorkerThread(stationTempMappingThread1, inputData.subList(0, DOC_COUNT/8)));
		Thread t2 = new Thread(new WorkerThread(stationTempMappingThread2, inputData.subList(DOC_COUNT/8, 2*DOC_COUNT/8)));
		Thread t3 = new Thread(new WorkerThread(stationTempMappingThread3, inputData.subList(2*DOC_COUNT/8, 3*DOC_COUNT/8)));
		Thread t4 = new Thread(new WorkerThread(stationTempMappingThread4, inputData.subList(3*DOC_COUNT/8, 4*DOC_COUNT/8)));
		Thread t5 = new Thread(new WorkerThread(stationTempMappingThread5, inputData.subList(4*DOC_COUNT/8, 5*DOC_COUNT/8)));
		Thread t6 = new Thread(new WorkerThread(stationTempMappingThread6, inputData.subList(5*DOC_COUNT/8, 6*DOC_COUNT/8)));
		Thread t7 = new Thread(new WorkerThread(stationTempMappingThread7, inputData.subList(6*DOC_COUNT/8, 7*DOC_COUNT/8)));
		Thread t8 = new Thread(new WorkerThread(stationTempMappingThread8, inputData.subList(7*DOC_COUNT/8, DOC_COUNT)));

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

		updateThreadMapping(stationTempMappingThread2);
		updateThreadMapping(stationTempMappingThread3);
		updateThreadMapping(stationTempMappingThread4);
		updateThreadMapping(stationTempMappingThread5);
		updateThreadMapping(stationTempMappingThread6);
		updateThreadMapping(stationTempMappingThread7);
		updateThreadMapping(stationTempMappingThread8);

		for (String stationId : stationTempMappingThread1.keySet()) {
			Station station = stationTempMappingThread1.get(stationId);
			station.computeAvg();
		}
		long endTime = System.currentTimeMillis();
		avgTime.add(endTime-startTime);
//		System.out.println("START-TIME:"+ startTime);
//		System.out.println("END-TIME:"+ endTime);
//		System.out.println("TOTAL-TIME:"+ (endTime-startTime));
		//printToFile();
	}

	private void updateThreadMapping(Map<String,Station> tempThreadMapping){
		for (String stnId : tempThreadMapping.keySet()) {
			Station tempStn = tempThreadMapping.get(stnId);
			if(stationTempMappingThread1.containsKey(stnId)){
				Station station = stationTempMappingThread1.get(stnId);
				station.appendTemp(tempStn.getTotalTemp());
				station.appendCount(tempStn.getTotalCount());
				stationTempMappingThread1.put(stnId, station);
				FibonaciUtil.fib(17);
			}else{
				stationTempMappingThread1.put(stnId, tempStn);
			}
		}
	}

	/**
	 * Prints the results to file. Used for debugging purpose
	 */
	private void printToFile() {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter("MultiThreadTempByStationShared.txt"));
			for (String stationId : stationTempMappingThread1.keySet()) {
				writer.write(stationId +" "+stationTempMappingThread1.get(stationId).getAvgTemp()+System.lineSeparator());
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
		System.out.println("**** MULTITHREADED AVG TEMPERATURE BY STATION HAVING INDIVIDUAL MAPS ********");
		System.out.println("AVG-TIME     : "+sumTime/10.0+" ms");
		System.out.println("SMALLEST-TIME: "+smallestTime+" ms");
		System.out.println("LONGEST-TIME : "+longestTime+" ms");

	}
	/**
	 * Worker thread that has a common shared data structure between all the threads that will be updated
	 * for each station value.
	 */
	class WorkerThread implements Runnable{
		Map<String,Station> sharedTempMapping;
		List<String> data;

		public WorkerThread(Map<String,Station> stationTempMapping, List<String> data){
			this.sharedTempMapping = stationTempMapping;
			this.data = data;
		}

		public void run() {
			for (String tempData : data) {
				String[] dataValues = tempData.split(",");
				if(dataValues[2].equals(TEMP_TYPE)){
					if(sharedTempMapping.containsKey(dataValues[0].trim())){
						Station station = sharedTempMapping.get(dataValues[0].trim());
						station.appendTemp(Integer.valueOf(dataValues[3].trim()));
						station.appendCount();
						sharedTempMapping.put(dataValues[0].trim(), station);
					}else{
						sharedTempMapping.put(dataValues[0].trim(), new Station(Integer.valueOf(dataValues[3].trim()),1));
					}
				}
			}
		}
	}
} 

