package com.dsc.iu.streaming;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TelemetryTestSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector spoutcollector;
	private String input;
	private ConcurrentLinkedQueue<String> nbqueue;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.spoutcollector = collector;
		try {
			nbqueue = new ConcurrentLinkedQueue<String>();
//			rdr = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\\\\\\\anomalydetection\\\\dixon_17000.log")));
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/scratch_ssd/sahil/dixon_indy34000.log")));
			String record;
			
//			while((record=bfrdr.readLine()) != null) {
//				if(record.startsWith("$P") && record.split("�")[2].length() >9) {
//					//o/p format eg: 5/28/17 00:00.00,202
//					nbqueue.add("5/28/17 " + record.split("�")[2] + "," + record.split("�")[record.split("�").length -3]);
//				}
//			}
			
			System.out.println("$$$$$$$$$$$ telemetry spout: "+ context.getThisTaskId() + "," + context.getThisComponentId() + "," + context.getThisTaskIndex() + "," +
								context.getThisWorkerPort() + ","+ context.getComponentIds());
			
			while((record=rdr.readLine()) != null) {
				nbqueue.add(record);
			}
			
			rdr.close();
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		
		if(nbqueue.size() >0) {
			spoutcollector.emit(new Values(nbqueue.poll()));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//declarer.declare(new Fields("record", "timestamp"));
		declarer.declare(new Fields("record"));
	}

}
