package com.dsc.iu.test.streaming;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SinkBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		int record = arg0.getIntegerByField("recordnum");
		double input = arg0.getDoubleByField("input");
		double score = arg0.getDoubleByField("score");
		
		System.out.println("************ sinkbolt: "+record+","+input+","+score);
	}

	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector arg2) {
		// TODO Auto-generated method stub
		
		System.out.println("%%%%%%%%%%%%%%% sinkbolt stats:" + context.getThisTaskId() + "," + context.getThisComponentId() + "," + context.getThisTaskIndex() 
							+ "," + context.getThisWorkerPort() + "," + context.getComponentIds());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
