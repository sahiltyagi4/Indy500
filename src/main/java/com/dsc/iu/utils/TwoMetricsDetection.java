package com.dsc.iu.utils;

import org.numenta.nupic.network.sensor.Publisher;

/*
 * combining multiple metrics for anomaly detection
 * */
public class TwoMetricsDetection {
	public static void main(String[] args) {
		Publisher manualPublisher = Publisher.builder().addHeader("speed,rpm").addHeader("float,float").addHeader("B").build();
	}
}
