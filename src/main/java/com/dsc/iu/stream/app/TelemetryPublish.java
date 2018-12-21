package com.dsc.iu.stream.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class TelemetryPublish implements MqttCallback {
	private MqttClient client;
	private MqttMessage msgobj = new MqttMessage();
	private String payload;
	
	public static void main(String[] args) {
		TelemetryPublish tp = new TelemetryPublish();
		tp.connectToBroker();
		tp.publishToBroker();
	}
	
	public void connectToBroker() {
		MqttConnectOptions conn = new MqttConnectOptions();
		
		//changing # of inflight messages from default 10 to 500 
		conn.setMaxInflight(500);
		
		conn.setAutomaticReconnect(true);
		conn.setCleanSession(true);
		conn.setConnectionTimeout(30);
		conn.setKeepAliveInterval(30);
		conn.setUserName("admin");
		conn.setPassword("password".toCharArray());
		
		try {
//			client = new MqttClient("tcp://127.0.0.1:61613", MqttClient.generateClientId());
			client = new MqttClient("tcp://10.16.0.73:61613", MqttClient.generateClientId());
			client.setCallback(this);
			client.connect(conn);
		} catch(MqttException m) {m.printStackTrace();}
	}
	
	public void publishToBroker() {
		try {
			List<String> carlist = new LinkedList<String>();
			carlist.add("20");carlist.add("21");
//			carlist.add("13");carlist.add("98");carlist.add("19");carlist.add("6");carlist.add("33");
//			carlist.add("24");carlist.add("26");carlist.add("7");carlist.add("60");carlist.add("27");carlist.add("17");carlist.add("15");
//			carlist.add("10");carlist.add("64");carlist.add("25");carlist.add("59");carlist.add("32");carlist.add("28");carlist.add("4");
//			carlist.add("3");carlist.add("18");carlist.add("22");carlist.add("12");carlist.add("1");carlist.add("9");carlist.add("14");
//			carlist.add("23");carlist.add("30");carlist.add("29");carlist.add("88");carlist.add("66");
			
			BufferedReader logreader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/scratch_ssd/sahil/IPBroadcaster_Input_2018-05-27_0.log"))));
			String record, topic;
			while((record=logreader.readLine()) != null) {
				//record.split("�")[1] denotes the car number. This acts as broker topic name specific to the log data for a given, subscribed by a dedicated spout
				if(record.startsWith("$P") && record.split("�")[2].length() >9) {
					topic = record.split("�")[1];
					if(carlist.contains(topic)) {
						//confirm the index for throttle metric in input logs
						// time_of_day, speed, rpm, throttle
//						payload = "5/27/18 " + record.split("�")[2] + "," + record.split("�")[4] + "," + record.split("�")[5] + "," + record.split("�")[6];
						payload = record.split("�")[4] + "," + record.split("�")[5] + "," + record.split("�")[6];
						System.out.println(payload + " for car no.: " + topic);
						msgobj.setQos(2);
						msgobj.setPayload(payload.getBytes());
						
						//client.publish(record.split("¦")[1], msgobj);
						client.publish(topic, msgobj);
					}
				}
			}
			
			logreader.close();
		} catch(IOException e) {
			e.printStackTrace();
		} catch(MqttException m) {
			m.printStackTrace();
		}
	}

	@Override
	public void connectionLost(Throwable arg0) {
		//auto-generated
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		//auto-generated
	}

	@Override
	public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
		//auto-generated
	}
}
