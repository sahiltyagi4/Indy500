package com.dsc.iu.streaming;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class AnomalyDetectionTest {
	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("eRPlog", new TelemetryTestSpout());
		builder.setBolt("htmbolt", new HTMBolt()).shuffleGrouping("eRPlog");
		
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("indy500", config, builder.createTopology());
		try {
			Thread.sleep(1000000);			//running topology for 1000 seconds in local mode
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
		cluster.shutdown();
	}
}
