package com.dsc.iu.report;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class PercentileSort {
	public static void main(String[] args) throws Exception {
		BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/sahiltyagi/Desktop/benchmarks/8htm/sorted_latency.csv")));
		String line; 
		int totalval=0, ctr=0;
		while((line =rdr.readLine()) != null) {
			totalval += Integer.parseInt(line.trim());
			ctr++;
		}
		
		System.out.println(totalval);
		System.out.println(ctr);
		System.out.println("average latency:" + (totalval/ctr));
	}
	
	public static void percentilesort() throws Exception {
		File f = new File("/Users/sahiltyagi/Desktop/benchmarks/1blank/sorted_latency.csv");
		PrintWriter pw  = new PrintWriter(f);
		List<Integer> list = new ArrayList<Integer>();
		BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/sahiltyagi/Desktop/benchmarks/1blank/spoutsinklatency.csv")));
		String line;
		while((line =rdr.readLine()) != null) {
			list.add(Integer.parseInt(line.split(",")[1].trim()));
		}
		
		rdr.close();
		System.out.println("set size:"+ list.size());
		Collections.sort(list);
		Iterator<Integer> itr = list.iterator();
		while(itr.hasNext()) {
			pw.println(itr.next());
		}
		
		pw.flush();
		pw.close();
		System.out.println("sorted latency file");
	}
}
