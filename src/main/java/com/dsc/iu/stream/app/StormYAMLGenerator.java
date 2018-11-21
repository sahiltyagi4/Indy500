package com.dsc.iu.stream.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class StormYAMLGenerator {
	private List<String> carslist;
	private List<String> metrics;
	private BufferedWriter writer;
	
	public static void main(String[] args) {
		StormYAMLGenerator yamlgen = new StormYAMLGenerator();
		yamlgen.composeYAML();
	}
	
	private void composeYAML() {
		try {
			carslist = new ArrayList<String>();
			metrics = new ArrayList<String>();
			
			//add car numbers to run topology on in the list
			carslist.add("9");
			//add metrics to run htm bolts on in the list
			metrics.add("speed");
			
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("D:\\\\anomalydetection\\indycar.yaml"))));
			
			StringBuilder spoutbldr = new StringBuilder();
			StringBuilder boltbldr = new StringBuilder();
			StringBuilder streambldr = new StringBuilder();
			
			writer.write("name: \"indycar-v2" + "\"\nconfig:\n  topology.workers: 1"  + "\n\n");
			
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}
