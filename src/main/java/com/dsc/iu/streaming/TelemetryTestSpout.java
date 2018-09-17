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
	private ConcurrentLinkedQueue<String> nbqueue;
	private SpoutOutputCollector spoutcollector;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		nbqueue = new ConcurrentLinkedQueue<String>();
		this.spoutcollector = collector;
		try {
//			BufferedReader bfrdr = new BufferedReader(new InputStreamReader
//					(new FileInputStream("/scratch_ssd/sahil/eRPGenerator_TGMLP_20170528_Indianapolis500_Race.log")));
			BufferedReader bfrdr = new BufferedReader(new InputStreamReader(new FileInputStream("/scratch_ssd/sahil/erp.log")));
			String record;
			//need to skip first 2 lines here?
			while((record=bfrdr.readLine()) != null) {
				if(record.startsWith("$P") && record.split("�")[2].length() >9) {
					//o/p format eg: 5/28/17 00:00.00,202
					nbqueue.add("5/28/17 " + record.split("�")[2] + "," + record.split("�")[record.split("�").length -3]);
				}
			}
			bfrdr.close();
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if(nbqueue.size()>0) {
			
			spoutcollector.emit(new Values(nbqueue.poll()));
		}
		
		//set input rate to 10 msg/sec
		//index $6
		try {
			Thread.sleep(100);
			
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//declarer.declare(new Fields("record", "timestamp"));
		declarer.declare(new Fields("record"));
	}

}
