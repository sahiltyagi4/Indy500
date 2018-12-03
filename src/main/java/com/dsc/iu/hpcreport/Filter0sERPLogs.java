package com.dsc.iu.hpcreport;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/*
 * removes 0s from erp telemetry logs so we can later manually inject 0s at labeled indices for car #9
 * only speed metric output
 * */
public class Filter0sERPLogs {
	public static void main(String[] args) {
		try {
			FileWriter fw = new FileWriter("C:\\Users\\styagi\\Desktop\\hpcreport\\anomaly11.csv");
			PrintWriter pw =new PrintWriter(fw);
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\styagi\\Desktop\\hpcreport\\anomalyscore11.csv")));
			String line;
			while((line=rdr.readLine()) != null) {
				pw.println(line.split(",")[0] + "," + line.split(",")[2]);
				pw.flush();
			}
			
			pw.close();
			fw.close();
			rdr.close();
			System.out.println("created record #, anomaly score file");
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}
