package com.dsc.iu.stream.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

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
		conn.setAutomaticReconnect(true);
		conn.setCleanSession(true);
		conn.setConnectionTimeout(30);
		conn.setKeepAliveInterval(30);
		conn.setUserName("admin");
		conn.setPassword("password".toCharArray());
		
		try {
			client = new MqttClient("tcp://127.0.0.1:61613", MqttClient.generateClientId());
			client.setCallback(this);
			client.connect(conn);
		} catch(MqttException m) {m.printStackTrace();}
	}
	
	public void publishToBroker() {
		try {
			BufferedReader logreader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/scratch_ssd/sahil/eRPGenerator_TGMLP_20170528_Indianapolis500_Race.log"))));
			String record;
			while((record=logreader.readLine()) != null) {
				//record.split("¦")[1] denotes the car number. This acts as broker topic name specific to the log data for a given, subscribed by a dedicated spout
				if(record.startsWith("$P") && record.split("¦")[2].length() >9) {
					//confirm the index for throttle metric in input logs
					payload = "5/28/17 " + record.split("¦")[2] + "," + record.split("¦")[4] + "," + record.split("¦")[5] + "," + record.split("¦")[6];
					msgobj.setQos(2);
					msgobj.setPayload(payload.getBytes());
					
					//client.publish(record.split("¦")[1], msgobj);
					client.publish("test1", msgobj);
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
