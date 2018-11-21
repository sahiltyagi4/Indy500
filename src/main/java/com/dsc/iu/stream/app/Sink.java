package com.dsc.iu.stream.app;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class Sink extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple arg0) {
		String carnum = arg0.getStringByField("carnum");
		String metric = arg0.getStringByField("metric");
		String data_val = arg0.getStringByField("dataval");
		double score = arg0.getDoubleByField("score");
		long ts = arg0.getLongByField("timestamp");
		
		//publish to pub/sub broker for correlation analysis
		System.out.println("******************** sink data: "+carnum+","+metric+","+data_val+","+score+","+ts);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

}
