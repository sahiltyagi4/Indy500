package com.dsc.iu.report;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.LinkedHashMap;
import java.util.Map;

/*
 * calculates the latency across each record index for the HTM anomaly score calculation in a storm topology.
 * The program aggregates the data generated across different executors running in storm and write data to 
 * file instead of default storm logs.
 * */
public class HTMExecutionFileWrite {
	public static void main(String[] args) {
		try {
			//make sure speed metric contains two decimals at least
			Map<String, Long> htmoutput = new LinkedHashMap<String, Long>();
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(
							new File("/Users/sahiltyagi/Desktop/htmsample.txt"))));
			String rec;
			while((rec=rdr.readLine()) != null) {
				String speed_metric=null;
				//rec.split(",")[]
				if(rec.split(",")[1].split("\\.")[1].length() == 1) {
					speed_metric = rec.split(",")[1] + "0";
				} else {
					speed_metric = rec.split(",")[1];
				}
				
				String key = String.valueOf((Integer.parseInt(rec.split(",")[0]) +1)) + "_" + speed_metric;
				
				long value = Long.parseLong(rec.split(",")[3]);
				htmoutput.put(key, value);
			}
			
//			for(Map.Entry<String, Long> set : htmoutput.entrySet()) {
//			System.out.println(set.getKey() + "," + set.getValue());
//			}
			
			rdr.close();
			System.out.println("htmoutput size:"+htmoutput.size());
			
			//speed metric contains three decimals in this file, last digit being zero. Removing it with substring.
			rdr = new BufferedReader(new InputStreamReader(new FileInputStream(
					new File("/Users/sahiltyagi/Desktop/executionTime.txt"))));
			Map<String, Long> executionTime = new LinkedHashMap<String, Long>();
			while((rec=rdr.readLine()) != null) {
				if(!rec.isEmpty()) {
					String speed_metric=null;
					speed_metric = rec.split(",")[1].substring(0, rec.split(",")[1].length() -1);
					String key = rec.split(",")[0] + "_" + speed_metric;
					
					long value = Long.parseLong(rec.split(",")[2]);
					executionTime.put(key, value);
				}
			}
			rdr.close();
			System.out.println("execution size:"+executionTime.size());
			//System.out.println(executionTime.get("8630_114.00"));
			for(Map.Entry<String, Long> set : executionTime.entrySet()) {
				System.out.println(set.getKey() + "," + set.getValue());
			}
			
			int index=0;
			BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/Users/sahiltyagi/Desktop/streamingHTM.csv"))));
			for(Map.Entry<String, Long> set : htmoutput.entrySet()) {
				index++;
				System.out.println(index + "," + (set.getValue() - executionTime.get(set.getKey())));
				wrtr.write(index + "," + (set.getValue() - executionTime.get(set.getKey())) + "\n");
				wrtr.flush();
			}
			
			wrtr.close();
			System.out.println("end HTMExecutionTopologyTime");
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}