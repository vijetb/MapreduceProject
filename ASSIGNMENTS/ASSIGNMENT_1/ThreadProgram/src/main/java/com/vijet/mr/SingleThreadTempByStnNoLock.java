package com.vijet.mr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single threaded Program that computes the Avg temperature of the station with no Lock on any data structure shared.
 * The program executes sequentially.
 */
public class SingleThreadTempByStnNoLock {
	private final static String TEMP_TYPE = "TMAX";
	private final Map<String,Station> stationTempMapping = new HashMap<String,Station>(); 
	private final List<String> inputData;
	private final List<Long> avgTime = new ArrayList<Long>();
	
	public SingleThreadTempByStnNoLock(List<String> data){
		this.inputData = data;
	}
	/**
	 * Computes the Avg temperature of all the station(10 iterations)
	 */
	public void computeAvgStnTemperature(){
		for(int i = 0 ; i < 10; i++){
			computeTemperature();
		}
	}
	/**
	 * Serially computes the temperatures by splitting the data reading line by line and updating 
	 * the map if the word is already encountered. Otherwise, creates a new entry in the map.
	 */
	public void computeTemperature(){
		stationTempMapping.clear();
		long startTime = System.currentTimeMillis();
		for (String data : inputData) {
			String[] dataValues = data.split(",");
			if(dataValues[2].equals(TEMP_TYPE)){
				if(stationTempMapping.containsKey(dataValues[0].trim())){
					Station station = stationTempMapping.get(dataValues[0].trim());
					station.appendTemp(Integer.valueOf(dataValues[3].trim()));
					station.appendCount();
					stationTempMapping.put(dataValues[0].trim(), station);
					FibonaciUtil.fib(17);
				}else{
					stationTempMapping.put(dataValues[0].trim(), new Station(Integer.valueOf(dataValues[3].trim()),1));
				}
			}
		}
		
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
			BufferedWriter writer = new BufferedWriter(new FileWriter("TempByStationNoLock.txt"));
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
	 * Prints the time to the console
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
		System.out.println("**** AVG TEMPERATURE BY STATION SEQUENTIAL ********");

		System.out.println("AVG-TIME     : "+sumTime/10.0+" ms");
		System.out.println("SMALLEST-TIME: "+smallestTime+" ms");
		System.out.println("LONGEST-TIME : "+longestTime+" ms");
	}
	
} 
