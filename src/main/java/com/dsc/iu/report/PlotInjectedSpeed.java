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

public class PlotInjectedSpeed {
	public static void main(String[] args) {
		try {
			BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
								new File("/Users/sahiltyagi/Desktop/injectedanomaly.csv"))));
			
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(
					new File("/Users/sahiltyagi/Desktop/htmoutput.txt"))));
			
			int index=0;
			String record;
			while((record =rdr.readLine()) != null) {
				if(record.contains(",0.00,")) {
					double input = Double.parseDouble(record.split(",")[1]);
					//index = Integer.parseInt(record.split(",")[0]);
					double anomalyscore = Double.parseDouble(record.split(",")[2]);
					index++;
					//wrtr.write(index + "," + anomalyscore + "\n");
					wrtr.write(input + "," + anomalyscore + ",\n");
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