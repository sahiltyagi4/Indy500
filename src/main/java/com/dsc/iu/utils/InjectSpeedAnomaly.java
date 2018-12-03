package com.dsc.iu.utils;

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
import java.util.Random;

/*
 * inserts anomalous data points at random indexes for a fixed seed value in indycar data corresponding to a particular car. 
 * The fraction of anomaly set to 0.5% => 540 points in 108,000 indycar dataset (car 8,10,11)
 * */
public class InjectSpeedAnomaly {
	
	public static void main(String[] args) {
		try {
			Random seed = new Random(7);
			BufferedReader rdr;
			String line;
			Map<Integer, String> anomalydatamap = new HashMap<Integer, String>();
			StringBuilder strbldr = new StringBuilder();
			BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("C:\\Users\\styagi\\Desktop\\hpcreport\\injected.log"))));
			rdr = new BufferedReader(new InputStreamReader(new FileInputStream(new File("C:\\Users\\styagi\\Desktop\\hpcreport\\nozeroes.log"))));
			while((line=rdr.readLine()) != null) {
				strbldr.append(line).append("\n");
			}
			
			strbldr = new StringBuilder(strbldr.toString().substring(0, strbldr.toString().length() -1));
			wrtr.write(strbldr.toString());
			wrtr.close();
			rdr.close();
			
			for(int i=0; i<540; i++) {
				int record_num = seed.nextInt(108000);
				rdr = new BufferedReader(new InputStreamReader(new FileInputStream(new File("C:\\Users\\styagi\\Desktop\\hpcreport\\injectedspeed.log"))));
				if(anomalydatamap.containsKey(record_num)) {
					record_num = seed.nextInt(108000);
				}
				
				//System.out.println("record index chosen is:"+record_num);
				for(int j=0; j<(record_num-1); j++) {
					line = rdr.readLine();
				}
				
				//System.out.println("anomalous record at index " + i + ":" + time + "," + "50.000" +"\n");
				if(anomalydatamap.containsKey(record_num)) {
					System.out.println("duplicate key present:"+record_num);
				} else {
					anomalydatamap.put(record_num, "0.000" +"\n");
				}
				
				rdr.close();
			}
			
			System.out.println("random  points generated:"+anomalydatamap.size());
			rdr = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/Users/sahiltyagi/Desktop/injected_anomaly.log"))));
			wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/Users/sahiltyagi/Desktop/anomalyInject1.log"))));
			int index=0;
			while((line = rdr.readLine()) != null) {
				index++;
				wrtr.write(line+"\n");
				if(anomalydatamap.containsKey(index)) {
					wrtr.write(anomalydatamap.get(index));
				}
			}
			rdr.close();
			wrtr.close();
			
			System.out.println("end Injecting speed anomaly of 0.000 mph in input datalogs");
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}