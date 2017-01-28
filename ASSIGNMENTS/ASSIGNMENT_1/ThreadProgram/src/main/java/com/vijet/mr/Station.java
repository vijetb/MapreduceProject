package com.vijet.mr;

/**
 * Bean class to store the Station details.
 */
public class Station {
	/**
	 * Total Temperature for particular Station
	 */
	private int totalTemp;
	/**
	 * Frequency
	 */
	private int totalCount;
	/**
	 * Avg. Temperature of this station
	 */
	private double avgTemp;
	
	public Station(int initialTemp, int count){
		this.totalTemp = initialTemp;
		this.totalCount = count;
	}
	
	public int getTotalTemp() {
		return totalTemp;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public double getAvgTemp() {
		return avgTemp;
	}
	
	/**
	 * Appends the temperature to the total Temperature
	 */
	public void appendTemp(int temp){
		totalTemp = totalTemp + temp;
	}
	/**
	 * Increments the count by 1
	 */
	public void appendCount(){
		totalCount = totalCount + 1;
	}
	/**
	 * Increments totalCount by 1
	 */
	public void appendCount(int count){
		totalCount = totalCount + count;
	}
	
	/**
	 * Computes the Average
	 */
	public void computeAvg(){
		avgTemp = totalTemp * 1.0/totalCount;
	}

	@Override
	public String toString() {
		return "Station [totalTemp=" + totalTemp + ", totalCount=" + totalCount
				+ ", avgTemp=" + avgTemp + "]";
	}	
}
