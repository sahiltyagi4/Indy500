package com.dsc.iu.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Parallel99SyncLatency {
	//baseline HTM for 33 cars on 3 metrics. Calculation of latency when a tuple gets synchronized across all 3 metrics for a car
	
	private static String fileloc = "/scratch_ssd/sahil/parallelsync/";
	private static String[] metrics = {"vehicleSpeed", "engineSpeed", "throttle"};
	private static Map<String, Long> outmap = new LinkedHashMap<String, Long>();
	private static Map<String, Long> inmap = new LinkedHashMap<String, Long>();
	
	public static void main(String[] args) throws IOException {
		Parallel99SyncLatency ob = new Parallel99SyncLatency();
		ob.syncLatency();
	}
	
	private void syncLatency() throws IOException {
//		String t = "13_2018-05-27T16:23:00.092Z";
//		System.out.println(t.replaceAll("T", " ").replaceAll("Z", "").trim());
		
		List<String> carlist = new ArrayList<String>();
		carlist.add("20");carlist.add("21");carlist.add("13");carlist.add("98");carlist.add("19");carlist.add("6");carlist.add("33");carlist.add("24");carlist.add("26");carlist.add("7");carlist.add("60");carlist.add("27");
		carlist.add("22");carlist.add("18");carlist.add("3");carlist.add("4");carlist.add("28");carlist.add("32");carlist.add("59");carlist.add("25");carlist.add("64");carlist.add("10");carlist.add("15");carlist.add("17");
		carlist.add("12");carlist.add("1");carlist.add("9");carlist.add("14");carlist.add("23");carlist.add("30");carlist.add("29");carlist.add("88");carlist.add("66");
		
		Iterator<String> itr = carlist.iterator();
		while(itr.hasNext()) {
			String carnum = itr.next();
			System.out.println("going with car-" + carnum);
			File latencyfile = new File("/scratch_ssd/sahil/latencysync/latency-" + carnum + ".csv");
			PrintWriter pw = new PrintWriter(latencyfile);
			
			for(String metric : metrics) {
				File f = new File(fileloc + "in-" + carnum + "-" + metric + ".csv");
				BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
				String line;
				while((line = rdr.readLine()) != null) {
					if(line.split(",").length == 3) {
						
						String inkey = line.split(",")[0];
						long in_ts = Long.parseLong(line.split(",")[2]);
						if(inmap.containsKey(inkey)) {
							if(in_ts < inmap.get(inkey)) {
								inmap.put(inkey, in_ts);
							}
						} else {
							inmap.put(inkey, in_ts);
						}
					}
				}
				rdr.close();
			}
			
			outfileprocessing(carnum);
			
			//inmap outmap iteration
			for(Map.Entry<String, Long> set : inmap.entrySet()) {
				String in_key = set.getKey();
				long in_val = set.getValue();
				
				if(outmap.containsKey(in_key)) {
					pw.println(in_key.split("_")[0] + "," + in_key.split("_")[1] + "," + (outmap.get(in_key) - in_val));
				}
			}
			
			inmap.clear();
			outmap.clear();
			pw.flush();
			pw.close();
		}
	}
	
	private static void outfileprocessing(String carnum) throws IOException {
		for(String metric : metrics) {
			File f = new File(fileloc + "out-" + carnum + "-" + metric + ".csv");
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String line;
			while((line = rdr.readLine()) != null) {
				if(line.split(",").length == 5) {
					String outkey = line.split(",")[0].replaceAll("T", " ").replaceAll("Z", "").trim();
					long aggregation_ts = Long.parseLong(line.split(",")[4]);
					if(outmap.containsKey(outkey)) {
						if(aggregation_ts > outmap.get(outkey)) {
							outmap.put(outkey, aggregation_ts);
						}
					} else {
						outmap.put(outkey, aggregation_ts);
					}
				}
			}
			rdr.close();
		}
	}
}
