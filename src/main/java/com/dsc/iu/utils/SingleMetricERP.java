package com.dsc.iu.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/*
 * fetches a single parameter from eRP log file, 'vehicle_speed'
 * */
public class SingleMetricERP {
	public static void main(String[] args) {
		try {
			System.out.println("going to start");
			BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\\\anomalydetection\\dixon_speed.log")));
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\\\anomalydetection\\eRPGenerator_TGMLP_20170528_Indianapolis500_Race.log")));
			String record;
			while((record = rdr.readLine()) != null) {
				if(record.startsWith("$P") && record.split("\\u00A6")[2].length() >9 && record.split("\\u00A6")[1].equals("9")) {
					//get all non-zero records for testing purposes
//					if(!record.split("\\u00A6")[4].equals("0.000")) {
						System.out.println(record.split("\\u00A6")[4]);
						wrtr.write(record.split("\\u00A6")[4] + "\n");
//					}
				}
			}
			
			System.out.println("generated the vehicle_speed log file for car #9");
			rdr.close();
			wrtr.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}
