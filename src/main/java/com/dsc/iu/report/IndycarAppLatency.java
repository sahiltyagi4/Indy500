package com.dsc.iu.report;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/*
 * calculates latency from instant of publishing to broker from ERP logs to fetching end results with anomaly scores on dashboard
 * */
public class IndycarAppLatency {

	public static void main(String[] args) {
		//for end to end pub to sub latency
		comparepubsubmaps();
	}
	
	private static HashMap<String, Long> procSubscriber() {
		HashMap<String, Long> sinkpubmap=null;
		try {
			sinkpubmap = new HashMap<String, Long>();
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/share/project/FG542/node4/recout/streamingtopic.csv")));
			String line;
			JSONParser parser = new JSONParser();
			while((line=rdr.readLine()) != null) {
				JSONObject json = (JSONObject)parser.parse(line.split("}")[0]+"}");
				sinkpubmap.put(json.get("UUID").toString(), Long.parseLong(line.split("}")[1].replaceAll(",", "").trim()));
			}
			
			rdr.close();
			System.out.println("size of recout streaming topic map:"+ sinkpubmap.size());
		} catch(Exception e) {
			e.printStackTrace();
		}
		return sinkpubmap;
	}
	
	private static HashMap<String, Long> procPublisher() {
		HashMap<String, Long> pubmap = new HashMap<String, Long>();
		try {
			File f = new File("/share/project/FG542/node4/recin/");
			if(f.isDirectory()) {
				File[] carfiles = f.listFiles();
				for(File crfile : carfiles) {
					BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(crfile)));
					String line;
					System.out.println(crfile.getName().split("-")[1].replaceAll(".csv", "").trim());
					while((line=rdr.readLine()) != null) {
						pubmap.put(crfile.getName().split("-")[1].replaceAll(".csv", "").trim() + "_" + line.split(",")[3], Long.parseLong(line.split(",")[6]));
					}
					rdr.close();
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return pubmap;
	}
	
	private static void comparepubsubmaps() {
		try {
			File f  = new File("/share/project/FG542/node4/pubsublatency.csv");
			PrintWriter pw = new PrintWriter(f);
			HashMap<String, Long> pubmap = procPublisher();
			HashMap<String, Long> submap= procSubscriber();
			System.out.println("size of pubmap:" + pubmap.size());
			System.out.println("size of submap:" + submap.size());
			for(Map.Entry<String, Long> entryset : submap.entrySet()) {
				String key = entryset.getKey();
				pw.println(key.split("_")[0] + "," + key.split("_")[1] + "," + entryset.getValue() + "," + pubmap.get(key) + "," 
							+ (entryset.getValue() - pubmap.get(key)));
			}
			
			pw.close();
			System.out.println("generated all latencies");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
}
