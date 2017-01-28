package com.vijet.mr;

/**
 * Utility class that computes the Fiboncai number
 */
public class FibonaciUtil {
	public static final int fib(int n){
		if(n==0|| n==1) return n;
		
		int[] values = new int[n];
		values[0] = 0;
		values[1] = 1;
		for(int i = 2; i<n;i++){
			values[i]=values[i-1]+values[i-2];
		}
		return values[n-1];
		
	}
}
