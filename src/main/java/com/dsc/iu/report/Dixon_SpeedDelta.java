package com.dsc.iu.report;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

/*
 * 
 * calculates the delta changes in the speed metric on a per record basis for a particular car (scott dixon's in this case)
 * 
 * */
public class Dixon_SpeedDelta {
	public static void main(String[] args) {
		try {
			Map<Integer, Double> speedmetricmap = new HashMap<Integer, Double>();
			
//			BufferedReader rdr = new BufferedReader(new InputStreamReader(
//						new FileInputStream("/Users/sahiltyagi/Desktop/Indy500/default_HTM/scott_dixon/dixon_indycar.log")));
			BufferedReader rdr = new BufferedReader(new InputStreamReader(
					new FileInputStream("/Users/sahiltyagi/Desktop/anomalyInject.log")));
			String rec;
			int index=0;
			while((rec=rdr.readLine()) != null) {
				index++;
				speedmetricmap.put(index, Double.parseDouble(rec.split(",")[1]));
			}
			rdr.close();
			
			BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/sahiltyagi/Desktop/delta.csv")));
			//manual record number inserted
//			for(int i=2; i<17008; i++) {
			for(int i=2; i<18114; i++) {
				System.out.println(i + "," + (speedmetricmap.get(i) - speedmetricmap.get(i-1)));
				wrtr.write(i + "," + (speedmetricmap.get(i) - speedmetricmap.get(i-1)) + "\n");
			}
			wrtr.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}
