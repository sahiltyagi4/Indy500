package com.dsc.iu.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class FIFOHTMjavaCheck {
	
	public static void main(String[] args) throws IOException {
		System.out.println("running car-1");
		LinkedList<String> input = readERP();
		confirmFIFOlogic(input);
	}
	
	private static void confirmFIFOlogic(LinkedList<String> list) throws IOException {
		File f = new File("/Users/sahiltyagi/Desktop/FIFOcheck.csv");
		PrintWriter pw = new PrintWriter(f);
		BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/sahiltyagi/Desktop/syncdata_aggr_allsub_sahil/input-6-9.csv")));
		String line;
		int index=0;
		while((line=rdr.readLine()) != null && line.split(",").length == 7) {
//			String record = line.split(",")[0] + "," + line.split(",")[1] + "," + line.split(",")[3] + "," + line.split(",")[5];
			String record = line.split(",")[0];
			
			if(index < list.size()) {
				String status;
				if(record.equals(list.get(index).split(",")[0])) {
					status = "yes";
				} else {
					status = "no";
					System.out.println(record + "," + list.get(index) + "," + status);
				}
				pw.println(record + "," + list.get(index) + "," + status);
				index++;
			}
		}
		rdr.close();
		pw.close();
	}
	
	private static LinkedList<String> readERP() throws IOException {
		LinkedList<String> list = new LinkedList<>();
		String erplog = "/Users/sahiltyagi/Downloads/Indy_500_2018/IPBroadcaster_Input_2018-05-27_0.log";
		BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(erplog)));
		String line;
		SimpleDateFormat df = new SimpleDateFormat("YYYY-mm-dd HH:mm:ss.SSS");
        Date dt=null;
        try {
      	  		dt = df.parse("2018-05-27 16:23:00.000");
        } catch(ParseException p) {}
        
        String carnum="6";
		while((line=rdr.readLine()) != null && line.trim().length() > 0) {
			if(line.startsWith("$P") && line.split("�")[2].matches("\\d+:\\d+:\\d+.\\d+") && line.split("�")[1].equalsIgnoreCase(carnum)) {
				
				String racetime = "2018-05-27 " + line.split("�")[2].trim();
				Date dtformat = null;
	  			try {
	  				dtformat = df.parse(racetime);
	  			} catch(ParseException p) {}
	  			
	  			if(dtformat.getTime() > dt.getTime()) {
	  				double speed  = Double.parseDouble(line.split("�")[4]);
	  				double rpm  = Double.parseDouble(line.split("�")[5]);
	  				double throttle  = Double.parseDouble(line.split("�")[6]);
	  				list.add(carnum + "_" + racetime+","+speed+","+rpm+","+throttle);
	  			}
			}
		}
		rdr.close();
		return list;
	}
}
