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

public class Concurrent33SyncLatency {
	//33 threads -> each thread running 3 htm networks
	
	private static String fileloc = "/scratch_ssd/sahil/parallelsync/";
	private static Map<String, Long> outmap = new LinkedHashMap<String, Long>();
	private static Map<String, Long> inmap = new LinkedHashMap<String, Long>();
	
	public static void main(String[] args) throws IOException {
		Concurrent33SyncLatency ob = new Concurrent33SyncLatency();
		ob.sync33threadLatency();
	}
	
	private void sync33threadLatency() throws IOException {
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
			inputdatamap(carnum);
			outputdatamap(carnum);
			
			for(Map.Entry<String, Long> set : outmap.entrySet()) {
				String key = set.getKey();
				if(inmap.containsKey(key)) {
					pw.println(key + "," + (set.getValue() - inmap.get(key)));
				}
			}
			pw.flush();
			pw.close();
			inmap.clear();
			outmap.clear();
			
		}
	}
	
	private static void inputdatamap(String carnum) throws IOException {
		File[] files = new File(fileloc).listFiles();
		for(File f : files) {
			if(f.getName().startsWith("input-")) {
				FileInputStream inp = new FileInputStream(f);
				BufferedReader in = new BufferedReader(new InputStreamReader(inp));
				String line;
				while ((line = in.readLine()) != null && line.split(",").length == 7) {
					if(carnum.equalsIgnoreCase(line.split("_")[0])) {
						String key = line.split(",")[0];
						//System.out.println("INmap key:" + key);
						inmap.put(key, Long.parseLong(line.split(",")[2]));
					}
				}
				in.close();
			}
		}
	}
	
	//INmap key:20_2018-05-27 19:24:50.658
	//20_2018-05-27 16:57:47.330
	private static void outputdatamap(String carnum) throws IOException {
		File[] files = new File(fileloc).listFiles();
		for(File f : files) {
			if(f.getName().startsWith("output-")) {
				FileInputStream inp = new FileInputStream(f);
				BufferedReader in = new BufferedReader(new InputStreamReader(inp));
				String line;
//				while ((line = in.readLine()) != null && line.split(",").length == 12) {
//					if(carnum.equalsIgnoreCase(line.split(",")[0])) {
//						String key = line.split(",")[0] + "_" + line.split(",")[1].replaceAll("T", " ").replaceAll("Z", "");
//						//System.out.println("OUTmap key:" + key);
//						outmap.put(key, Long.parseLong(line.split(",")[11]));
//					}
//				}
				
				while ((line = in.readLine()) != null && line.split(",").length == 9) {
					if(carnum.equalsIgnoreCase(line.split(",")[0])) {
						String key = line.split(",")[0] + "_" + line.split(",")[1].replaceAll("T", " ").replaceAll("Z", "");
						//System.out.println("OUTmap key:" + key);
						outmap.put(key, Long.parseLong(line.split(",")[8]));
					}
				}
				
				in.close();
			}
		}
	}
}
