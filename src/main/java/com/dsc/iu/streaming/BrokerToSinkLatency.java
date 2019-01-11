package com.dsc.iu.streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class BrokerToSinkLatency {
	public static void main(String[] args) throws IOException {
		
		File out = new File("/N/u/styagi/brokersinklatency.csv");
		PrintWriter pw  = new PrintWriter(out);
		Map<String, Long> brokermap = new HashMap<String, Long>();
		Map<String, Long> sinkmap = new HashMap<String, Long>();
		BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/N/u/styagi/pubsublogs.txt")));
		String line;
		while((line=rdr.readLine()) != null) {
			brokermap.put("speed_"+line.split(",")[3], Long.parseLong(line.split(",")[4]));
			brokermap.put("RPM_"+line.split(",")[3], Long.parseLong(line.split(",")[4]));
			brokermap.put("throttle_"+line.split(",")[3], Long.parseLong(line.split(",")[4]));
		}
		rdr.close();
		
		rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/N/u/styagi/sinkfile.txt")));
		while((line=rdr.readLine()) != null) {
			sinkmap.put(line.split(",")[1], Long.parseLong(line.split(",")[2]));
		}
		rdr.close();
		
		for(Map.Entry<String, Long> set : brokermap.entrySet()) {
			String brokerkey = set.getKey();
			long brokerval = set.getValue();
			if(!sinkmap.containsKey(brokerkey)) {
				System.out.println("following data missing in sink:" + brokerkey);
			} else {
				pw.println(brokerkey + "," + (sinkmap.get(brokerkey) - brokerval));
			}
		}
		
		pw.flush();
		pw.close();
		System.out.println("calculated broker to sink latency");
	}
}
