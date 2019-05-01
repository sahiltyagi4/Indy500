package com.dsc.iu.streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StreamingLatency {
//	String loc = "/Users/sahiltyagi/Desktop/eScience-2019/storm-2nodes/33cars/run2";
	String loc = "/Users/sahiltyagi/Desktop/eScience-2019/storm-1node/33cars";
	
	public static void main(String[] args) {
		StreamingLatency ob = new StreamingLatency();
		
//		LinkedHashMap<String, Long> pubmap = ob.realpublisherfile();
//		LinkedHashMap<String, Long> sinkmap = ob.readsinkfiles();
//		LinkedHashMap<String, Long> spoutmap = ob.readspoutfiles();
//		ob.calculatestreaminglatency(pubmap, sinkmap, spoutmap);
//		ob.carwiselatency();
		
		ob.calculateCCDF();
		ob.minmaxavg();
		
		System.out.println("completed.");
	}
	
	public LinkedHashMap<String, Long> readspoutfiles() {
		LinkedHashMap<String, Long> spoutmap = new LinkedHashMap<>();
		try {
			File dir = new File("/N/u/styagi/combinedspouts");
			File[] files = dir.listFiles();
			for(File f : files) {
				BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
				String line;
				while((line=rdr.readLine()) != null) {
//					if(line.split(",").length == 3) {
					if(line.split(",").length == 6) {
						spoutmap.put(line.split(",")[0] + "_" + line.split(",")[1], Long.parseLong(line.split(",")[2]));
					}
				}
				
				System.out.println("done reading spout file:" + f.getName());
				rdr.close();
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("size of spoutmap:" + spoutmap.size());
		return spoutmap;
	}
	
	public LinkedHashMap<String, Long> realpublisherfile() {
		LinkedHashMap<String, Long> pubmap = new LinkedHashMap<>();
		try {
//			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(
//							"/Users/sahiltyagi/Desktop/eScience-2019/storm-2nodes/1car/publish1-2nodes.csv")));
			
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/scratch_ssd/sahil/publisher.csv")));
			//File dir = new File("/Users/sahiltyagi/Desktop/eScience-2019/storm-2nodes/1car/car-13.csv");
			//PrintWriter pw = new PrintWriter(dir);
			String line;
			while((line=rdr.readLine()) != null) {
//				if(line.split(",").length == 3 && line.split(",")[0].equals("13")) {
				if(line.split(",").length == 3) {
					pubmap.put(line.split(",")[0] + "_" + line.split(",")[1], Long.parseLong(line.split(",")[2]));
					//pw.println(line);
				}
			}
			rdr.close();	
			//pw.flush();
			//pw.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
		System.out.println("size of pubmap:" + pubmap.size());
		return pubmap;
	}
	
	public LinkedHashMap<String, Long> readsinkfiles() {
		LinkedHashMap<String, Long> sinkmap = new LinkedHashMap<>();
		try {
			File dir = new File("/N/u/styagi/combinedsinks");
			File[] files = dir.listFiles();
			for(File f : files) {
				BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
				String line;
				while((line=rdr.readLine()) != null) {
					if(line.split(",").length ==8) {
						sinkmap.put(line.split(",")[0], Long.parseLong(line.split(",")[7]));
					}
				}
				
				System.out.println("done reading sink file:" + f.getName());
				rdr.close();
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("size of sinkmap:" + sinkmap.size());
		return sinkmap;
	}
	
	public void calculatestreaminglatency(LinkedHashMap<String, Long> pubmap, LinkedHashMap<String, Long> sinkmap, LinkedHashMap<String, Long> spoutmap) {
		try {
			//18,2018-05-27 18:18:26.536
			//additional stats like mean, min and max
			int ctr=1;
			long total=0L;
			List<Long> latencies = new ArrayList<>();
			
			File f = new File("/N/u/styagi/stormlatency.csv");
			PrintWriter pw = new PrintWriter(f);
			
			for(Map.Entry<String, Long> entry : sinkmap.entrySet()) {
				String key = entry.getKey();
				Long value = entry.getValue();
//				System.out.println("key:" + key);
//				System.out.println("value:" + value);
//				System.out.println("pubmap val:" + pubmap.get(key));
				if(pubmap.containsKey(key) && spoutmap.containsKey(key)) {
					if((value - pubmap.get(key) > 0)) {
						String data = key.split("_")[0] + "," + key.split("_")[1] + "," + (value - pubmap.get(key) + "," + (value - spoutmap.get(key)));
						pw.println(data);
						
						total = total + (value - pubmap.get(key));
						latencies.add((value - pubmap.get(key)));
						ctr++;
					}
				}
			}
			pw.flush();
			pw.close();
//			System.out.println("average is:" + (total/ctr));
//			System.out.println("maximum value:" + Collections.max(latencies));
//			System.out.println("minimum value:" + Collections.min(latencies));
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void carwiselatency() {
		List<String> carlist = new ArrayList<>();
		carlist.add("20");carlist.add("21");carlist.add("13");carlist.add("98");carlist.add("19");carlist.add("6");carlist.add("33");carlist.add("24");carlist.add("26");carlist.add("7");carlist.add("60");carlist.add("27");
		carlist.add("22");carlist.add("18");carlist.add("3");carlist.add("4");carlist.add("28");carlist.add("32");carlist.add("59");carlist.add("25");carlist.add("64");carlist.add("10");carlist.add("15");carlist.add("17");
		carlist.add("12");carlist.add("1");carlist.add("9");carlist.add("14");carlist.add("23");carlist.add("30");carlist.add("29");carlist.add("88");carlist.add("66");
		try {
			for(String car : carlist) {
				File f = new File("/N/u/styagi/latencystorm2nodes/latency-" + car + ".csv");
				PrintWriter pw = new PrintWriter(f);
				BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/N/u/styagi/stormlatency.csv")));
				String line;
				while((line=rdr.readLine()) != null) {
					if(line.split(",")[0].equals(car)) {
						pw.println(line);
					}
				}
				rdr.close();
				pw.flush();
				pw.close();
				System.out.println("completed for car:" + car);
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void calculateCCDF() {
		try {
			LinkedHashMap<Double, Double> cdfmap = new LinkedHashMap<>();
			for(int i=0; i< 401; i++) {
				if(i % 5 == 0 && i!=0) {
					cdfmap.put(Double.valueOf(i), 0.);
				}
			}
			
			int totalrecords=0;
			
			File f = new File(loc + "/stormlatency.csv");
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String line;
			
			while((line = rdr.readLine()) != null) {
				//System.out.println(line);
				double latency = Double.parseDouble(line.split(",")[3]);
				for(Map.Entry<Double, Double> entry : cdfmap.entrySet()) {
					double current_window = entry.getKey();
					if(latency >= current_window) {
						double value = entry.getValue() + 1;
						cdfmap.put(current_window, value);	
					}
				}
				totalrecords++;
			}
			rdr.close();
			
			File cdf = new File(loc + "/ccdf.csv");
			PrintWriter pw = new PrintWriter(cdf);
			for(Map.Entry<Double, Double> entry : cdfmap.entrySet()) {
				pw.println(entry.getKey() + "," + Double.valueOf(entry.getValue()/totalrecords));
			}
			pw.flush();
			pw.close();
			System.out.println("totalrecords: " + totalrecords);
			System.out.println("written cdf to file");
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void minmaxavg() {
		try {
			File f = new File(loc + "/stormlatency.csv");
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String line;
			List<Integer> latencylist= new ArrayList<>();
			int totaltime=0, ctr=0;
			
			while((line = rdr.readLine()) != null) {
				latencylist.add(Integer.parseInt(line.split(",")[3]));
				totaltime += Integer.parseInt(line.split(",")[3]);
				ctr++;
			}
			rdr.close();
			
			System.out.println("average:" + (totaltime/ctr));
			System.out.println(Collections.max(latencylist));
			System.out.println(Collections.min(latencylist));
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}
