package com.dsc.iu.streaming;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

public class SpoutToSinkLatency {
	public static void main(String[] args) throws IOException {
		Map<String, Long> sinkmap = new HashMap<>();
		Map<String, Long> spoutmap = new HashMap<>();
		BufferedReader rdr;
		String line;
		BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/N/u/styagi/spoutsinklatency.csv")));
		
		if(new File("/N/u/styagi/spoutdata").isDirectory()) {
			
			File[] spoutsdata = new File("/N/u/styagi/spoutdata").listFiles();
			for(File spout: spoutsdata) {
				rdr = new BufferedReader(new InputStreamReader(new FileInputStream(spout)));
				while((line=rdr.readLine()) != null) {
					spoutmap.put(line.split(",")[1], Long.parseLong(line.split(",")[4]));
					spoutmap.put(line.split(",")[2], Long.parseLong(line.split(",")[4]));
					spoutmap.put(line.split(",")[3], Long.parseLong(line.split(",")[4]));
				}
				rdr.close();
			}
		}
		
		rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/N/u/styagi/sinkfile.csv")));
		while((line=rdr.readLine()) != null) {
			sinkmap.put(line.split(",")[1], Long.parseLong(line.split(",")[2]));
		}
		rdr.close();
		
		for(Map.Entry<String, Long> entryset : spoutmap.entrySet()) {
			String key = entryset.getKey();
			if(sinkmap.containsKey(key)) {
				wrtr.write(key + "," + (sinkmap.get(key) - entryset.getValue()) + "\n");
				System.out.println("k");
			} else {
				System.out.println("not found key:"+key);
			}
		}
		wrtr.close();
	}
}
