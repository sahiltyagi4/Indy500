package com.dsc.iu.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class TopologyPerformance {
	public static void main(String[] args) {
		
		try {
			
//			String q = "apache-storm-1.0.4/logs/workers-artifacts/indy500-1-1536696594/6800/worker.log:2018-09-11 16:10:42.974 "
//					+ "STDIO Thread-6-htmbolt-executor[3 3] [INFO] $$$$$$$$$$$$$$$$$$$$$$$$$$,16,221.07,1536696642974";
//			System.out.println(q.substring(q.lastIndexOf("]")+1, q.length()));
			
			
			File f = new File("/Users/sahiltyagi/Desktop/executiontime.txt");
			PrintWriter pw = new PrintWriter(f);
			
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/sahiltyagi/Desktop/recordindex.txt")));
			Map<Integer, Long> exectimemap = new HashMap<Integer, Long>();
			String st,s;
			while((st=rdr.readLine()) != null) {
				if(!st.isEmpty()) {
					s = st.substring(st.lastIndexOf("]")+1, st.length()).trim();
					//System.out.println(s);
					exectimemap.put(Integer.parseInt(s.split(",")[1]), Long.parseLong(s.split(",")[3]));
				}
			}
			rdr.close();
			
			rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/sahiltyagi/Desktop/htmoutput.txt")));
			while((st=rdr.readLine()) != null) {
				s = st.substring(st.lastIndexOf("]")+1, st.length()).trim();
				Integer index = Integer.parseInt(s.split(",")[1]);
				Long ts = Long.parseLong(s.split(",")[4]);
				//handle redundancy of partially running experiments causing mismatch of # records flushed to each file
				if(exectimemap.containsKey(index)) {
					pw.println(index + "," + (ts - exectimemap.get(index)));
				}
			}
			
			System.out.println("completed.");
			pw.close();
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}
