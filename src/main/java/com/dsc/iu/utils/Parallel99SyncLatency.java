package com.dsc.iu.utils;

public class Parallel99SyncLatency {
	//baseline HTM for 33 cars on 3 metrics. Calculation of latency when a tuple gets synchronized across all 3 metrics for a car
	
	public static void main(String[] args) {
		Parallel99SyncLatency ob = new Parallel99SyncLatency();
		ob.syncLatency();
	}
	
	private void syncLatency() {
		String t = "13_2018-05-27T16:23:00.092Z";
		System.out.println(t.replaceAll("T", " ").replaceAll("Z", "").trim());
	}
}
