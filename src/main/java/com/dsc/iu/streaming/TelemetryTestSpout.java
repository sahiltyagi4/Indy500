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
	private static BufferedReader rdr;
	private String input;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.spoutcollector = collector;
		try {
			rdr = new BufferedReader(new InputStreamReader(new FileInputStream("/N/u/styagi/dixon_indycar.log")));
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		
		try {
			while((input=rdr.readLine()) != null) {
				spoutcollector.emit(new Values(input));
//				Thread.sleep(100);
			}
		} catch(IOException e) {
			e.printStackTrace();
		} 
//		catch(InterruptedException e) {
//			e.printStackTrace();
//		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//declarer.declare(new Fields("record", "timestamp"));
		declarer.declare(new Fields("record"));
	}

}
