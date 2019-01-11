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

public class BoltToSinkLatency {
	public static void main(String[] args) throws IOException {
		Map<String, Long> sinkmap = new HashMap<>();
		Map<String, Long> boltmap = new HashMap<>();
		BufferedReader rdr;
		String line;
		BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/N/u/styagi/boltsinklatency.csv")));
		
		if(new File("/N/u/styagi/boltdata").isDirectory()) {
			
			File[] boltsdata = new File("/N/u/styagi/boltdata").listFiles();
			for(File bolt: boltsdata) {
				rdr = new BufferedReader(new InputStreamReader(new FileInputStream(bolt)));
				while((line=rdr.readLine()) != null) {
					boltmap.put(line.split(",")[1], Long.parseLong(line.split(",")[4]));
				}
				rdr.close();
			}
		}
		
		rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/N/u/styagi/sinkfile.csv")));
		while((line=rdr.readLine()) != null) {
			sinkmap.put(line.split(",")[1], Long.parseLong(line.split(",")[2]));
		}
		rdr.close();
		
		for(Map.Entry<String, Long> entryset : boltmap.entrySet()) {
			String key = entryset.getKey();
			if(sinkmap.containsKey(key)) {
				wrtr.write(key + "," + (sinkmap.get(key) - entryset.getValue())+"\n");
			} else {
				System.out.println("bot found key:"+key);
			}
		}
		wrtr.close();
	}
}
