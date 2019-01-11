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

public class SpoutToBoltLatency {
	public static void main(String[] args) throws IOException {
		Map<String, Long> spoutmap = new HashMap<>();
		Map<String, Long> boltmap = new HashMap<>();
		BufferedReader rdr;
		String line;
		BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/N/u/styagi/spoutboltlatency.csv")));
		
		if(new File("/N/u/styagi/spoutdata").isDirectory()) {
			
			File[] spoutsdata = new File("/N/u/styagi/spoutdata").listFiles();
			for(File spout : spoutsdata) {
				rdr = new BufferedReader(new InputStreamReader(new FileInputStream(spout)));
				while((line=rdr.readLine()) != null) {
					spoutmap.put(line.split(",")[1], Long.parseLong(line.split(",")[4]));
					spoutmap.put(line.split(",")[2], Long.parseLong(line.split(",")[4]));
					spoutmap.put(line.split(",")[3], Long.parseLong(line.split(",")[4]));
				}
				rdr.close();
			}
		}
		
		if(new File("/N/u/styagi/boltdata").isDirectory()) {
			
			File[] boltsdata = new File("/N/u/styagi/boltdata").listFiles();
			for(File bolt : boltsdata) {
				rdr = new BufferedReader(new InputStreamReader(new FileInputStream(bolt)));
				while((line=rdr.readLine()) != null) {
					boltmap.put(line.split(",")[1], Long.parseLong(line.split(",")[4]));
				}
				rdr.close();
			}
		}
		
		for(Map.Entry<String, Long> set : spoutmap.entrySet()) {
			
			String key = set.getKey();
			if(boltmap.containsKey(key)) {
				wrtr.write(key + "," + (set.getValue() - boltmap.get(key))+"\n");
			} else {
				System.out.println("not found following key:" + key);
			}
		}
		
		wrtr.close();
	}
}
