package com.dsc.iu.report;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;

public class PlotInjectedAnomalyScore {
	public static void main(String[] args) {
		try {
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(
								new File("/Users/sahiltyagi/Desktop/anomalyInject1.log"))));
			BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
								new File("/Users/sahiltyagi/Desktop/injectedanomaly.csv"))));
			Set<Integer> indexeset = new HashSet<Integer>();
			
			String record;
			int index=0;
			while((record=rdr.readLine()) !=null) {
				index++;
//				if(record.split(",")[1].equals("0.000")) {
//					indexeset.add(index);
//				}
				
				if(record.split(",")[1].contains("-")) {
					indexeset.add(index);
				}
			}
			
			rdr.close();
			rdr = new BufferedReader(new InputStreamReader(new FileInputStream(
					new File("/Users/sahiltyagi/Desktop/htmsample.txt"))));
			
			int index2=0;
			while((record =rdr.readLine()) != null) {
				index = Integer.parseInt(record.split(",")[0]);
				if(indexeset.contains(index)) {
					index2++;
					wrtr.write(index2 + "," + record.split(",")[2] + "," + index + "\n");
				}
			}
			
			rdr.close();
			wrtr.close();
			System.out.println("complete fetching indexes of injected anomalies");
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}
